package com.kafkick.core.admin.couponmetrics;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataErrorCode;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;
import com.kafkick.core.admin.queue.AdminQueueObservationSource;
import com.kafkick.core.admin.queue.CampaignQueueObservation;
import com.kafkick.core.admin.stock.AdminStockResolver;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 관리자 캠페인 상세 지표의 기준 시각·원천 조회·순수 계산 흐름을 조립합니다.
 *
 * <p>DB 권위값은 기술 중립 {@link AdminCampaignDataReader}에서 읽고, OPEN 캠페인의 발급률은
 * {@link CouponIssuanceRateReader}에서 읽고, OPEN 캠페인의 대기열은
 * {@link AdminQueueObservationSource}에서 읽습니다.</p>
 */
public class AdminCouponMetricsService {

    private final TimeProvider timeProvider;
    private final AdminCampaignDataReader campaignDataReader;
    private final CouponIssuanceRateReader issuanceRateReader;
    private final AdminQueueObservationSource queueObservationSource;
    private final CouponMetricsCalculator calculator;
    private final AdminStockResolver stockResolver;

    /**
     * 한 요청의 시간과 상세 원천 및 계산을 담당할 협력 객체를 주입받습니다.
     *
     * @param timeProvider 요청 전체에서 한 번 사용할 기준 시각 공급자
     * @param campaignDataReader Overview와 같은 DB 모집단의 상세 조회 경계
     * @param issuanceRateReader OPEN 캠페인의 Prometheus 초당 발급률 조회 경계
     * @param queueObservationSource OPEN 캠페인의 대기열 관측 조회 경계
     * @param calculator 원천값을 요청 구간의 상세 지표로 변환하는 순수 계산기
     */
    public AdminCouponMetricsService(
            TimeProvider timeProvider,
            AdminCampaignDataReader campaignDataReader,
            CouponIssuanceRateReader issuanceRateReader,
            AdminQueueObservationSource queueObservationSource,
            CouponMetricsCalculator calculator
    ) {
        this(timeProvider, campaignDataReader, issuanceRateReader, queueObservationSource, calculator,
                new AdminStockResolver(AdminStockResolver.unavailableV2Reader()));
    }

    /** 운영 배선에서 Overview와 같은 회차별 재고 원천 선택기를 함께 주입받습니다. */
    public AdminCouponMetricsService(
            TimeProvider timeProvider,
            AdminCampaignDataReader campaignDataReader,
            CouponIssuanceRateReader issuanceRateReader,
            AdminQueueObservationSource queueObservationSource,
            CouponMetricsCalculator calculator,
            AdminStockResolver stockResolver
    ) {
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.campaignDataReader = Objects.requireNonNull(campaignDataReader, "campaignDataReader");
        this.issuanceRateReader = Objects.requireNonNull(issuanceRateReader, "issuanceRateReader");
        this.queueObservationSource = Objects.requireNonNull(queueObservationSource, "queueObservationSource");
        this.calculator = Objects.requireNonNull(calculator, "calculator");
        this.stockResolver = Objects.requireNonNull(stockResolver, "stockResolver");
    }

    /**
     * 현재 시점의 한 캠페인 상세 지표를 요청 구간에 맞춰 반환합니다.
     *
     * @param couponId 조회할 쿠폰 ID
     * @param window 발급률과 상태 전이율 계산 구간
     * @return 계산 완료된 캠페인 상세 지표
     * @throws BusinessException 캠페인이 없거나 DB 관측이 불가능한 경우
     */
    public CouponMetricsSnapshot getCouponMetrics(long couponId, MetricsWindow window) {
        Objects.requireNonNull(window, "window");
        // 한 응답의 모든 원천과 계산 경계가 같도록 현재 시각을 최초 한 번만 읽습니다.
        Instant snapshotAt = timeProvider.instant();
        Instant fromInclusive = snapshotAt.minus(window.duration());
        AdminCampaignDetailData detail = stockResolver.resolve(campaignDataReader.findDetail(
                couponId, fromInclusive, snapshotAt, snapshotAt), snapshotAt);
        if (detail.availability() == DetailAvailability.NOT_FOUND) {
            throw new BusinessException(
                    CommonErrorCode.NOT_FOUND, "상세 지표 캠페인을 찾을 수 없습니다: " + couponId);
        }
        if (detail.availability() == DetailAvailability.UNAVAILABLE) {
            throw new BusinessException(AdminCampaignDataErrorCode.OBSERVATION_UNAVAILABLE);
        }
        AdminCampaignDetailData.DetailValue value = detail.value();
        CouponMetricsSource source = new CouponMetricsSource(
                value.couponId(), value.campaign(), value.stock(), issuanceRate(value, window, snapshotAt),
                queue(value, fromInclusive, snapshotAt),
                value.holdingCounts(), value.transitions());
        return calculator.calculate(source, window, snapshotAt);
    }

    /** OPEN 상태일 때만 외부 rate를 읽고 그 밖의 캠페인은 값 없는 N_A로 고정합니다. */
    private CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> issuanceRate(
            AdminCampaignDetailData.DetailValue value,
            MetricsWindow window,
            Instant snapshotAt
    ) {
        if (value.campaign().status() != CouponRoundStatus.OPEN) {
            // 비 OPEN 캠페인은 관측계 질의 자체를 만들지 않아 N_A 의미를 보존합니다.
            return notApplicable();
        }
        return issuanceRateReader.read(value.couponId(), window, snapshotAt);
    }

    /** OPEN 캠페인의 요청 구간 대기열을 읽고 공통 관측을 상세 계산기 입력으로 변환합니다. */
    private CouponMetricsSource.Observation<CouponMetricsSource.QueueCounts> queue(
            AdminCampaignDetailData.DetailValue value,
            Instant windowStart,
            Instant snapshotAt
    ) {
        if (value.campaign().status() != CouponRoundStatus.OPEN) {
            // 비 OPEN 캠페인은 대기열 기능의 적용 대상이 아니므로 원천에 질의하지 않습니다.
            return notApplicable();
        }
        java.util.Map<Long, CampaignQueueObservation> observations = queueObservationSource.observe(
                List.of(value.couponId()), windowStart, snapshotAt, snapshotAt);
        CampaignQueueObservation observation = observations == null ? null : observations.get(value.couponId());
        if (observations == null || observations.size() != 1 || observation == null
                || !observations.keySet().equals(java.util.Set.of(value.couponId()))
                || value.couponId() != observation.couponId()) {
            throw new BusinessException(
                    AdminCampaignDataErrorCode.OBSERVATION_UNAVAILABLE,
                    "대기열 관측 응답이 현재 상세 캠페인과 일치해야 합니다.");
        }
        if (!observation.sourceStatus().carriesValue()) {
            return new CouponMetricsSource.Observation<>(null, observation.sourceStatus(), null);
        }
        return new CouponMetricsSource.Observation<>(
                new CouponMetricsSource.QueueCounts(
                        observation.currentWaitingCount(), observation.admittedCount(),
                        observation.windowStart(), observation.windowEnd()),
                observation.sourceStatus(), observation.observedAt());
    }

    /** 비 OPEN 캠페인에 값을 싣지 않는 N_A 발급률 원천을 만듭니다. */
    private static <T> CouponMetricsSource.Observation<T> notApplicable() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.N_A, null);
    }
}

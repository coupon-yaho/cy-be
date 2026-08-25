package com.kafkick.core.admin.couponmetrics;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataErrorCode;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 관리자 캠페인 상세 지표의 기준 시각·원천 조회·순수 계산 흐름을 조립합니다.
 *
 * <p>DB 권위값은 기술 중립 {@link AdminCampaignDataReader}에서 읽고, OPEN 캠페인의 발급률은
 * {@link CouponIssuanceRateReader}에서 읽습니다. 대기열은 이번 상세 범위에 포함하지 않아
 * {@link SourceStatus#PENDING}으로 계산기에 전달합니다.</p>
 */
public class AdminCouponMetricsService {

    private final TimeProvider timeProvider;
    private final AdminCampaignDataReader campaignDataReader;
    private final CouponIssuanceRateReader issuanceRateReader;
    private final CouponMetricsCalculator calculator;

    /**
     * 한 요청의 시간과 상세 원천 및 계산을 담당할 협력 객체를 주입받습니다.
     *
     * @param timeProvider 요청 전체에서 한 번 사용할 기준 시각 공급자
     * @param campaignDataReader Overview와 같은 DB 모집단의 상세 조회 경계
     * @param issuanceRateReader OPEN 캠페인의 Prometheus 초당 발급률 조회 경계
     * @param calculator 원천값을 요청 구간의 상세 지표로 변환하는 순수 계산기
     */
    public AdminCouponMetricsService(
            TimeProvider timeProvider,
            AdminCampaignDataReader campaignDataReader,
            CouponIssuanceRateReader issuanceRateReader,
            CouponMetricsCalculator calculator
    ) {
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
        this.campaignDataReader = Objects.requireNonNull(campaignDataReader, "campaignDataReader");
        this.issuanceRateReader = Objects.requireNonNull(issuanceRateReader, "issuanceRateReader");
        this.calculator = Objects.requireNonNull(calculator, "calculator");
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
        AdminCampaignDetailData detail = campaignDataReader.findDetail(
                couponId, fromInclusive, snapshotAt, snapshotAt);
        if (detail.availability() == DetailAvailability.NOT_FOUND) {
            throw new BusinessException(
                    CommonErrorCode.NOT_FOUND, "상세 지표 캠페인을 찾을 수 없습니다: " + couponId);
        }
        if (detail.availability() == DetailAvailability.UNAVAILABLE) {
            throw new BusinessException(AdminCampaignDataErrorCode.OBSERVATION_UNAVAILABLE);
        }
        AdminCampaignDetailData.DetailValue value = detail.value();
        CouponMetricsSource source = new CouponMetricsSource(
                value.couponId(), value.campaign(), value.stock(), issuanceRate(value, window, snapshotAt), pending(),
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

    /** 아직 연결되지 않은 상세 원천을 0이 아닌 값 없는 PENDING으로 보존합니다. */
    private static <T> CouponMetricsSource.Observation<T> pending() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.PENDING, null);
    }

    /** 비 OPEN 캠페인에 값을 싣지 않는 N_A 발급률 원천을 만듭니다. */
    private static <T> CouponMetricsSource.Observation<T> notApplicable() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.N_A, null);
    }
}

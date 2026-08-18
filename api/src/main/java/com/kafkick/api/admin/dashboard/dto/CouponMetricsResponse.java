package com.kafkick.api.admin.dashboard.dto;

import java.time.Instant;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.coupon.CouponStatus;

/**
 * 특정 쿠폰 캠페인의 재고·발급·대기열·보유 상태를 동일 snapshot 기준으로 반환하는 응답 초안입니다.
 *
 * <p>독립적으로 실패할 수 있는 수치는 {@link ObservedValue}로 감싸 원천 상태와 관측 시각을 함께 보존합니다.
 * DB 재고가 있어도 Redis 대기열이 미수집이면 해당 항목만 {@code PENDING}으로 표현하며 0으로 대체하지 않습니다.
 * {@code window}와 전이 집계도 명시적인 enum·record로 고정해 임의 문자열이나 비정형 객체를 허용하지 않습니다.</p>
 *
 * @param couponId 쿠폰 캠페인 회차 식별자
 * @param snapshotAt 응답 지표의 기준 시각
 * @param window 집계 구간
 * @param stock 최초·잔여 재고 요약
 * @param issuanceProgress 전체 수량 대비 발급 진행률
 * @param issuanceRate 현재·최고 초당 발급량
 * @param queue 대기 인원과 예상 대기 시간
 * @param campaign 권위 DB의 캠페인 실행 상태
 * @param usageRatio 발급 후 사용 완료 비율
 * @param holdingCounts 발급권 상태별 보유 건수
 * @param transitionRate 사용·취소·만료 전이별 집계
 */
public record CouponMetricsResponse(
        Long couponId,
        Instant snapshotAt,
        MetricsWindow window,
        StockSummary stock,
        ObservedValue<Double> issuanceProgress,
        ObservedValue<RateSummary> issuanceRate,
        QueueSummary queue,
        CampaignRuntimeSummary campaign,
        ObservedValue<Double> usageRatio,
        ObservedValue<IssuanceStatusCounts> holdingCounts,
        ObservedValue<TransitionRateSummary> transitionRate
) {

    /**
     * 아직 원천이 연결되지 않은 쿠폰 지표 응답 예시를 만듭니다.
     *
     * @param couponId 예시에 사용할 쿠폰 캠페인 회차 식별자
     * @param window 예시에 사용할 집계 구간
     * @return 수집 지표가 PENDING/null인 쿠폰 지표 응답
     */
    public static CouponMetricsResponse draft(Long couponId, MetricsWindow window) {
        // 직렬화 초안에서는 미수집 수치를 0으로 만들지 않고 PENDING/null 조합으로 표현합니다.
        ObservedValue<Long> pendingCount = new ObservedValue<>(null, SourceStatus.PENDING, null);
        return new CouponMetricsResponse(
                couponId,
                Instant.EPOCH,
                window,
                new StockSummary(pendingCount, pendingCount),
                new ObservedValue<>(null, SourceStatus.PENDING, null),
                new ObservedValue<>(null, SourceStatus.PENDING, null),
                new QueueSummary(pendingCount, pendingCount),
                null,
                new ObservedValue<>(null, SourceStatus.PENDING, null),
                new ObservedValue<>(null, SourceStatus.PENDING, null),
                new ObservedValue<>(null, SourceStatus.PENDING, null)
        );
    }

    /**
     * 최초 발행 가능 수량과 현재 잔여 수량을 원천 상태와 함께 제공합니다.
     *
     * @param initialCount 캠페인 최초 발행 가능 수량
     * @param remainingCount 현재 잔여 수량
     */
    public record StockSummary(ObservedValue<Long> initialCount, ObservedValue<Long> remainingCount) { }

    /**
     * 현재 초당 발급량과 관측 구간 내 최고 초당 발급량입니다.
     *
     * @param currentPerSecond 현재 초당 발급량
     * @param peakPerSecond 집계 구간 내 최고 초당 발급량
     */
    public record RateSummary(double currentPerSecond, double peakPerSecond) { }

    /**
     * 현재 대기 인원과 예상 대기 시간을 각각 독립 관측값으로 제공합니다.
     *
     * @param waitingCount 현재 대기 인원
     * @param estimatedWaitMillis 예상 대기 시간(ms)
     */
    public record QueueSummary(ObservedValue<Long> waitingCount, ObservedValue<Long> estimatedWaitMillis) { }

    /**
     * 권위 DB의 캠페인 상태와 실제 오픈 시각이며 오픈 전이면 openedAt은 null일 수 있습니다.
     *
     * @param status 권위 DB의 캠페인 상태
     * @param openedAt 실제 오픈 시각; 오픈 전이면 null
     */
    public record CampaignRuntimeSummary(CouponStatus status, Instant openedAt) { }

    /**
     * 발급권의 현재 상태별 보유 건수입니다.
     *
     * @param issuedCount 발급 완료 후 미사용 건수
     * @param usedCount 사용 완료 건수
     * @param cancelledCount 취소 건수
     * @param expiredCount 만료 건수
     */
    public record IssuanceStatusCounts(long issuedCount, long usedCount, long cancelledCount, long expiredCount) { }

    /**
     * 발급 이후 각 상태 전이의 집계 건수를 제공합니다.
     *
     * @param useCount 사용 완료 전이 건수
     * @param cancelUseCount 사용 취소 전이 건수
     * @param cancelCount 발급 취소 전이 건수
     * @param expireCount 만료 전이 건수
     */
    public record TransitionRateSummary(long useCount, long cancelUseCount, long cancelCount, long expireCount) { }
}

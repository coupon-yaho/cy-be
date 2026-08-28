package com.kafkick.core.admin.couponmetrics;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/**
 * 관리자 캠페인 상세 화면에 전달할 계산 완료 결과입니다.
 *
 * <p>HTTP 표현과 분리된 Core 계약이며, 모든 계산값은 원천 상태와 관측 시각을 함께 유지합니다.</p>
 *
 * @param couponId 조회한 쿠폰 ID
 * @param snapshotAt 요청에서 한 번 정한 전체 기준 시각
 * @param window 발급률과 전이율 계산 구간
 * @param stock 최초 수량과 잔여 수량
 * @param issuanceProgress 전체 수량 대비 활성 발급 진행 비율
 * @param issuanceRate 현재 및 구간 최고 초당 발급 수
 * @param queue 현재 대기 수와 예상 대기시간
 * @param campaign 캠페인 상태와 오픈 시각
 * @param usageRatio 발급·사용 상태 중 사용 비율
 * @param holdingCounts 발급 상태별 현재 보유량
 * @param transitionRate 구간별 초당 상태 전이 수
 */
public record CouponMetricsSnapshot(
        long couponId,
        Instant snapshotAt,
        MetricsWindow window,
        StockSummary stock,
        Observation<Double> issuanceProgress,
        Observation<RateSummary> issuanceRate,
        QueueSummary queue,
        CampaignRuntimeSummary campaign,
        Observation<Double> usageRatio,
        Observation<IssuanceStatusCounts> holdingCounts,
        Observation<TransitionRateSummary> transitionRate
) {

    /** 결과의 필수 구성요소와 식별자를 검증합니다. */
    public CouponMetricsSnapshot {
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(stock, "stock");
        Objects.requireNonNull(issuanceProgress, "issuanceProgress");
        Objects.requireNonNull(issuanceRate, "issuanceRate");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(campaign, "campaign");
        Objects.requireNonNull(usageRatio, "usageRatio");
        Objects.requireNonNull(holdingCounts, "holdingCounts");
        Objects.requireNonNull(transitionRate, "transitionRate");
        requireRatio(issuanceProgress, "issuanceProgress");
        requireRatio(usageRatio, "usageRatio");
    }

    /** 최초 재고와 현재 잔여 재고입니다. */
    public record StockSummary(Observation<Long> initialCount, Observation<Long> remainingCount) {

        /** 재고 관측값 두 항목이 모두 존재하는지 검증합니다. */
        public StockSummary {
            Objects.requireNonNull(initialCount, "initialCount");
            Objects.requireNonNull(remainingCount, "remainingCount");
        }
    }

    /** 현재 및 요청 구간 최고 초당 발급 수입니다. */
    public record RateSummary(double currentPerSecond, double peakPerSecond) {

        /** 발급률이 음수가 아니고 유한한지 검증합니다. */
        public RateSummary {
            requireRate(currentPerSecond, "currentPerSecond");
            requireRate(peakPerSecond, "peakPerSecond");
        }
    }

    /** 현재 대기 수와 예상 대기시간입니다. */
    public record QueueSummary(Observation<Long> waitingCount, Observation<Duration> estimatedWait) {

        /** 대기열 결과 두 항목이 모두 존재하는지 검증합니다. */
        public QueueSummary {
            Objects.requireNonNull(waitingCount, "waitingCount");
            Objects.requireNonNull(estimatedWait, "estimatedWait");
            if (estimatedWait.value() != null && estimatedWait.value().isNegative()) {
                throw new IllegalArgumentException("estimatedWait는 음수일 수 없습니다.");
            }
        }
    }

    /** 캠페인의 현재 운영 상태와 설정된 오픈 시각입니다. */
    public record CampaignRuntimeSummary(CouponRoundStatus status, Instant opensAt) {

        /** 캠페인 실행 정보를 검증합니다. */
        public CampaignRuntimeSummary {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(opensAt, "opensAt");
        }
    }

    /** 발급된 쿠폰의 현재 상태별 보유량입니다. */
    public record IssuanceStatusCounts(long issued, long used, long cancelled, long expired) {

        /** 모든 보유량이 음수가 아닌지 검증합니다. */
        public IssuanceStatusCounts {
            requireNonNegative(issued, "issued");
            requireNonNegative(used, "used");
            requireNonNegative(cancelled, "cancelled");
            requireNonNegative(expired, "expired");
        }
    }

    /** 요청 구간에 계산한 초당 상태 전이율입니다. */
    public record TransitionRateSummary(
            double usePerSecond,
            double cancelUsePerSecond,
            double cancelPerSecond,
            double expirePerSecond
    ) {

        /** 모든 전이율이 음수가 아니고 유한한지 검증합니다. */
        public TransitionRateSummary {
            requireRate(usePerSecond, "usePerSecond");
            requireRate(cancelUsePerSecond, "cancelUsePerSecond");
            requireRate(cancelPerSecond, "cancelPerSecond");
            requireRate(expirePerSecond, "expirePerSecond");
        }
    }

    /**
     * 계산값과 원천 상태·실제 관측 시각을 함께 보존합니다.
     *
     * @param <T> 계산값 타입
     * @param value 계산값; 값을 싣지 않는 상태에서는 null
     * @param status 원천에서 전파한 관측 상태
     * @param observedAt 해당 값을 만든 원천의 실제 관측 시각
     */
    public record Observation<T>(T value, SourceStatus status, Instant observedAt) {

        /** 공통 상태 분류와 값·시각 조합을 검증하고 비율 범위를 보호합니다. */
        public Observation {
            Objects.requireNonNull(status, "status");
            if (status.carriesValue()) {
                if (value == null || observedAt == null) {
                    throw new IllegalArgumentException(status + " 상태에는 value와 observedAt이 필요합니다.");
                }
            } else if (value != null || observedAt != null) {
                throw new IllegalArgumentException(status + " 상태의 value와 observedAt은 null이어야 합니다.");
            }
        }
    }

    /** 초당 비율 필드가 음수가 아닌 유한값인지 확인합니다. */
    private static void requireRate(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0) {
            throw new IllegalArgumentException(name + "은 음수가 아닌 유한값이어야 합니다.");
        }
    }

    /** 값이 있는 비율 결과가 0.0 이상 1.0 이하의 유한값인지 확인합니다. */
    private static void requireRatio(Observation<Double> observation, String name) {
        if (observation.value() == null) {
            return;
        }
        double value = observation.value();
        if (!Double.isFinite(value) || value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(name + "은 0.0 이상 1.0 이하여야 합니다.");
        }
    }

    /** 수량 결과가 음수가 아닌지 확인합니다. */
    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + "은 음수일 수 없습니다.");
        }
    }
}

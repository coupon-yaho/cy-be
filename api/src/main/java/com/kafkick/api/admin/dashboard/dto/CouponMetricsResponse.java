package com.kafkick.api.admin.dashboard.dto;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot;
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

    /** 응답 식별자·필수 영역과 비율 범위를 HTTP DTO 생성 경계에서 검증합니다. */
    public CouponMetricsResponse {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(stock, "stock");
        Objects.requireNonNull(issuanceProgress, "issuanceProgress");
        Objects.requireNonNull(issuanceRate, "issuanceRate");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(usageRatio, "usageRatio");
        Objects.requireNonNull(holdingCounts, "holdingCounts");
        Objects.requireNonNull(transitionRate, "transitionRate");
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        requireRatio(issuanceProgress, "issuanceProgress");
        requireRatio(usageRatio, "usageRatio");
    }

    /**
     * Core 계산 결과를 값·상태·관측 시각 손실 없이 HTTP 응답으로 변환합니다.
     *
     * @param snapshot 계산 완료된 기술 중립 캠페인 상세 지표
     * @return 관리자 상세 지표 HTTP 응답
     */
    public static CouponMetricsResponse from(CouponMetricsSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        return new CouponMetricsResponse(
                snapshot.couponId(),
                snapshot.snapshotAt(),
                snapshot.window(),
                new StockSummary(
                        observed(snapshot.stock().initialCount()),
                        observed(snapshot.stock().remainingCount())),
                observed(snapshot.issuanceProgress()),
                observed(snapshot.issuanceRate(), rate -> new RateSummary(
                        rate.currentPerSecond(), rate.peakPerSecond())),
                new QueueSummary(
                        observed(snapshot.queue().waitingCount()),
                        observed(snapshot.queue().estimatedWait(), Duration::toMillis)),
                new CampaignRuntimeSummary(
                        snapshot.campaign().status(), snapshot.campaign().opensAt()),
                observed(snapshot.usageRatio()),
                observed(snapshot.holdingCounts(), counts -> new IssuanceStatusCounts(
                        counts.issued(), counts.used(), counts.cancelled(), counts.expired())),
                observed(snapshot.transitionRate(), rate -> new TransitionRateSummary(
                        rate.usePerSecond(), rate.cancelUsePerSecond(),
                        rate.cancelPerSecond(), rate.expirePerSecond())));
    }

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
    public record StockSummary(ObservedValue<Long> initialCount, ObservedValue<Long> remainingCount) {

        /** 두 재고 관측값의 존재와 음수 수량을 검증합니다. */
        public StockSummary {
            Objects.requireNonNull(initialCount, "initialCount");
            Objects.requireNonNull(remainingCount, "remainingCount");
            requireCount(initialCount, "initialCount");
            requireCount(remainingCount, "remainingCount");
        }
    }

    /**
     * 현재 초당 발급량과 관측 구간 내 최고 초당 발급량입니다.
     *
     * @param currentPerSecond 현재 초당 발급량
     * @param peakPerSecond 집계 구간 내 최고 초당 발급량
     */
    public record RateSummary(double currentPerSecond, double peakPerSecond) {

        /** 발급률이 음수가 아닌 유한값인지 검증합니다. */
        public RateSummary {
            requireRate(currentPerSecond, "currentPerSecond");
            requireRate(peakPerSecond, "peakPerSecond");
        }
    }

    /**
     * 현재 대기 인원과 예상 대기 시간을 각각 독립 관측값으로 제공합니다.
     *
     * @param waitingCount 현재 대기 인원
     * @param estimatedWaitMillis 예상 대기 시간(ms)
     */
    public record QueueSummary(ObservedValue<Long> waitingCount, ObservedValue<Long> estimatedWaitMillis) {

        /** 대기 수와 예상 대기시간 관측값의 존재와 음수 수량을 검증합니다. */
        public QueueSummary {
            Objects.requireNonNull(waitingCount, "waitingCount");
            Objects.requireNonNull(estimatedWaitMillis, "estimatedWaitMillis");
            requireCount(waitingCount, "waitingCount");
            requireCount(estimatedWaitMillis, "estimatedWaitMillis");
        }
    }

    /**
     * 권위 DB의 캠페인 상태와 설정된 오픈 시각입니다.
     *
     * @param status 권위 DB의 캠페인 상태
     * @param opensAt 설정된 캠페인 오픈 시각
     */
    public record CampaignRuntimeSummary(CouponStatus status, Instant opensAt) {

        /** 캠페인 상태가 항상 존재하는지 검증합니다. */
        public CampaignRuntimeSummary {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(opensAt, "opensAt");
        }
    }

    /**
     * 발급권의 현재 상태별 보유 건수입니다.
     *
     * @param unusedCount 발급 완료 후 미사용 건수
     * @param usedCount 사용 완료 건수
     * @param cancelledCount 취소 건수
     * @param expiredCount 만료 건수
     */
    public record IssuanceStatusCounts(long unusedCount, long usedCount, long cancelledCount, long expiredCount) {

        /** 모든 상태별 보유량이 음수가 아닌지 검증합니다. */
        public IssuanceStatusCounts {
            requireCount(unusedCount, "unusedCount");
            requireCount(usedCount, "usedCount");
            requireCount(cancelledCount, "cancelledCount");
            requireCount(expiredCount, "expiredCount");
        }
    }

    /**
     * 발급 이후 각 상태 전이의 초당 발생률을 제공합니다.
     *
     * @param usePerSecond 초당 사용 완료 전이 수
     * @param cancelUsePerSecond 초당 사용 취소 전이 수
     * @param cancelPerSecond 초당 발급 취소 전이 수
     * @param expirePerSecond 초당 만료 전이 수
     */
    public record TransitionRateSummary(
            double usePerSecond,
            double cancelUsePerSecond,
            double cancelPerSecond,
            double expirePerSecond
    ) {

        /** 모든 상태 전이율이 음수가 아닌 유한값인지 검증합니다. */
        public TransitionRateSummary {
            requireRate(usePerSecond, "usePerSecond");
            requireRate(cancelUsePerSecond, "cancelUsePerSecond");
            requireRate(cancelPerSecond, "cancelPerSecond");
            requireRate(expirePerSecond, "expirePerSecond");
        }
    }

    /** Core Observation의 값·상태·시각을 같은 값 타입의 HTTP 관측값으로 옮깁니다. */
    private static <T> ObservedValue<T> observed(CouponMetricsSnapshot.Observation<T> source) {
        return observed(source, Function.identity());
    }

    /** 값이 있는 상태에서만 변환 함수를 적용하고 상태·관측 시각은 그대로 보존합니다. */
    private static <S, T> ObservedValue<T> observed(
            CouponMetricsSnapshot.Observation<S> source,
            Function<S, T> mapper
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(mapper, "mapper");
        T value = source.value() == null ? null : mapper.apply(source.value());
        return new ObservedValue<>(value, source.status(), source.observedAt());
    }

    /** 값이 있는 비율 관측값이 0.0 이상 1.0 이하의 유한값인지 확인합니다. */
    private static void requireRatio(ObservedValue<Double> observation, String name) {
        if (observation.value() == null) {
            return;
        }
        double value = observation.value();
        if (!Double.isFinite(value) || value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(name + "은 0.0 이상 1.0 이하여야 합니다.");
        }
    }

    /** 값이 있는 long 관측값이 음수가 아닌지 확인합니다. */
    private static void requireCount(ObservedValue<Long> observation, String name) {
        if (observation.value() != null) {
            requireCount(observation.value(), name);
        }
    }

    /** long 수량이 음수가 아닌지 확인합니다. */
    private static void requireCount(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + "은 음수일 수 없습니다.");
        }
    }

    /** 초당 비율이 음수가 아닌 유한값인지 확인합니다. */
    private static void requireRate(double value, String name) {
        if (!Double.isFinite(value) || value < 0.0) {
            throw new IllegalArgumentException(name + "은 음수가 아닌 유한값이어야 합니다.");
        }
    }
}

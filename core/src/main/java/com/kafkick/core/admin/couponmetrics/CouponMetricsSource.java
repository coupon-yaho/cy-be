package com.kafkick.core.admin.couponmetrics;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

/**
 * 관리자 캠페인 상세 지표 계산에 전달하는 기술 중립 원천값입니다.
 *
 * <p>DB나 Redis 자료구조를 노출하지 않고, 값과 관측 상태 및 관측 시각을 함께 보존합니다.
 * 누적 발급 표본과 상태 전이 버킷은 생성 시 불변 복사하여 요청 중 원천값이 바뀌지 않게 합니다.</p>
 *
 * @param couponId 캠페인을 식별하는 양수 쿠폰 ID
 * @param campaign 캠페인 운영 상태와 오픈 시각
 * @param stock 전체 수량과 활성 발급 수량
 * @param issuanceSamples 누적 발급 완료 Counter 표본
 * @param queue 현재 대기 수와 입장 관측 구간
 * @param holdingCounts 발급 상태별 현재 보유량
 * @param transitions 사용·취소·만료 상태 전이 버킷
 */
public record CouponMetricsSource(
        Long couponId,
        CampaignRuntime campaign,
        Observation<StockCounts> stock,
        Observation<List<IssuanceCounterSample>> issuanceSamples,
        Observation<QueueCounts> queue,
        Observation<IssuanceStatusCounts> holdingCounts,
        Observation<List<TransitionBucket>> transitions
) {

    /** 원천값 전체의 필수 항목과 시계열 순서를 검증합니다. */
    public CouponMetricsSource {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(campaign, "campaign");
        Objects.requireNonNull(stock, "stock");
        Objects.requireNonNull(issuanceSamples, "issuanceSamples");
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(holdingCounts, "holdingCounts");
        Objects.requireNonNull(transitions, "transitions");
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }

        if (issuanceSamples.value() != null) {
            List<IssuanceCounterSample> samples = List.copyOf(issuanceSamples.value());
            validateSamples(samples);
            issuanceSamples = new Observation<>(samples,
                    issuanceSamples.status(), issuanceSamples.observedAt());
        }
        if (transitions.value() != null) {
            List<TransitionBucket> buckets = List.copyOf(transitions.value());
            validateTransitionBuckets(buckets);
            transitions = new Observation<>(buckets,
                    transitions.status(), transitions.observedAt());
        }
    }

    /** 누적 Counter가 시간순으로 증가하거나 유지되는지 확인합니다. */
    private static void validateSamples(List<IssuanceCounterSample> samples) {
        for (int index = 1; index < samples.size(); index++) {
            IssuanceCounterSample previous = samples.get(index - 1);
            IssuanceCounterSample current = samples.get(index);
            if (!current.observedAt().isAfter(previous.observedAt())) {
                throw new IllegalArgumentException("발급 Counter 표본 시각은 오름차순이어야 합니다.");
            }
            if (current.cumulativeCompletedCount() < previous.cumulativeCompletedCount()) {
                throw new IllegalArgumentException("누적 발급 완료 수는 감소할 수 없습니다.");
            }
        }
    }

    /** 전달된 순서에서 상태 전이 버킷이 서로 겹치지 않는지 확인합니다. */
    private static void validateTransitionBuckets(List<TransitionBucket> buckets) {
        for (int index = 1; index < buckets.size(); index++) {
            TransitionBucket previous = buckets.get(index - 1);
            TransitionBucket current = buckets.get(index);
            if (current.windowStart().isBefore(previous.windowEnd())) {
                throw new IllegalArgumentException("상태 전이 버킷은 겹칠 수 없습니다.");
            }
        }
    }

    /** 캠페인의 실행 상태와 실제 오픈 시각입니다. */
    public record CampaignRuntime(CouponStatus status, Instant openedAt) {

        /** 필수 캠페인 정보를 검증합니다. */
        public CampaignRuntime {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(openedAt, "openedAt");
        }
    }

    /** 전체 발급 가능 수량과 현재 활성 발급 수량입니다. */
    public record StockCounts(long totalQuantity, long activeCount) {

        /** 재고 수량의 범위와 포함 관계를 검증합니다. */
        public StockCounts {
            if (totalQuantity < 0L || activeCount < 0L || activeCount > totalQuantity) {
                throw new IllegalArgumentException("재고 수량 관계가 유효하지 않습니다.");
            }
        }
    }

    /** 특정 시각의 누적 발급 완료 Counter 표본입니다. */
    public record IssuanceCounterSample(Instant observedAt, long cumulativeCompletedCount) {

        /** 표본 시각과 누적 수량을 검증합니다. */
        public IssuanceCounterSample {
            Objects.requireNonNull(observedAt, "observedAt");
            requireNonNegative(cumulativeCompletedCount, "cumulativeCompletedCount");
        }
    }

    /** 현재 대기 수와 최근 입장 처리 관측 구간입니다. */
    public record QueueCounts(
            long waitingCount,
            long admittedCount,
            Instant windowStart,
            Instant windowEnd
    ) {

        /** 대기·입장 수량과 양수 관측 구간을 검증합니다. */
        public QueueCounts {
            requireNonNegative(waitingCount, "waitingCount");
            requireNonNegative(admittedCount, "admittedCount");
            requirePositiveWindow(windowStart, windowEnd, "대기열 입장 관측 구간");
        }
    }

    /** 발급된 쿠폰의 현재 상태별 보유량입니다. */
    public record IssuanceStatusCounts(long issued, long used, long cancelled, long expired) {

        /** 모든 상태별 보유량이 음수가 아닌지 검증합니다. */
        public IssuanceStatusCounts {
            requireNonNegative(issued, "issued");
            requireNonNegative(used, "used");
            requireNonNegative(cancelled, "cancelled");
            requireNonNegative(expired, "expired");
        }
    }

    /** 일정 구간에 발생한 사용·사용취소·취소·만료 전이 수입니다. */
    public record TransitionBucket(
            Instant windowStart,
            Instant windowEnd,
            long use,
            long cancelUse,
            long cancel,
            long expire
    ) {

        /** 버킷 구간과 전이 수량을 검증합니다. */
        public TransitionBucket {
            requirePositiveWindow(windowStart, windowEnd, "상태 전이 버킷");
            requireNonNegative(use, "use");
            requireNonNegative(cancelUse, "cancelUse");
            requireNonNegative(cancel, "cancel");
            requireNonNegative(expire, "expire");
        }
    }

    /**
     * 원천값과 그 값을 해석할 상태·관측 시각을 함께 보존합니다.
     *
     * @param <T> 원천값 타입
     * @param value 실제 값; 값을 싣지 않는 상태에서는 null
     * @param status 값의 관측 상태
     * @param observedAt 실제 관측 시각; 값을 싣지 않는 상태에서는 null
     */
    public record Observation<T>(T value, SourceStatus status, Instant observedAt) {

        /** 공통 상태 분류에 맞는 값·시각 조합인지 검증합니다. */
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

    /** 수량 필드가 음수가 아닌지 확인합니다. */
    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + "은 음수일 수 없습니다.");
        }
    }

    /** 시작보다 종료가 늦은 양수 시간 구간인지 확인합니다. */
    private static void requirePositiveWindow(Instant start, Instant end, String name) {
        Objects.requireNonNull(start, "windowStart");
        Objects.requireNonNull(end, "windowEnd");
        if (!end.isAfter(start)) {
            throw new IllegalArgumentException(name + "은 양수 시간 구간이어야 합니다.");
        }
    }
}

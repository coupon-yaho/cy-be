package com.kafkick.core.admin.couponmetrics;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.CouponRoundRuntimeSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.IssuanceStatusCounts;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.Observation;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.QueueSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.RateSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.StockSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.TransitionRateSummary;
import com.kafkick.core.observation.SourceStatus;

/**
 * 쿠폰 회차 상세 원천값을 요청 구간에 맞는 재고·발급·대기·사용 지표로 계산합니다.
 *
 * <p>외부 저장소나 HTTP 타입에 의존하지 않는 순수 계산기입니다. 원천이 값을 싣지 않는 상태면
 * 계산을 시도하지 않고 같은 상태를 결과에 보존합니다.</p>
 */
@Component
public class CouponMetricsCalculator {

    /**
     * 한 쿠폰 회차의 상세 지표 스냅샷을 계산합니다.
     *
     * @param source 상태와 관측 시각을 포함한 쿠폰 회차 원천값
     * @param window 발급률·전이율 계산 구간
     * @param snapshotAt 요청 전체에서 공유하는 기준 시각
     * @return 계산 완료된 기술 중립 상세 지표
     * @throws IllegalArgumentException 미래 표본, 부족한 발급 표본 또는 재고·보유량 불일치가 있는 경우
     */
    public CouponMetricsSnapshot calculate(
            CouponMetricsSource source,
            MetricsWindow window,
            Instant snapshotAt
    ) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        validateHoldingConsistency(source.stock(), source.holdingCounts());

        Instant windowStart = snapshotAt.minus(window.duration());
        return new CouponMetricsSnapshot(
                source.couponId(),
                snapshotAt,
                window,
                stock(source.stock()),
                issuanceProgress(source.stock()),
                issuanceRate(source.issuanceRateSamples(), windowStart, snapshotAt),
                queue(source.queue()),
                new CouponRoundRuntimeSummary(source.couponRound().status(), source.couponRound().opensAt()),
                usageRatio(source.holdingCounts()),
                holdingCounts(source.holdingCounts()),
                transitionRates(source.transitions(), windowStart, snapshotAt));
    }

    /** 재고 원천을 최초 수량과 잔여 수량으로 변환합니다. */
    private static StockSummary stock(
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> source
    ) {
        if (!source.status().carriesValue()) {
            return new StockSummary(empty(source.status()), empty(source.status()));
        }
        long remaining = Math.subtractExact(
                source.value().totalQuantity(), source.value().activeCount());
        return new StockSummary(
                observed(source.value().totalQuantity(), source),
                observed(remaining, source));
    }

    /** 전체 수량 대비 현재 활성 발급 수량의 비율을 계산합니다. */
    private static Observation<Double> issuanceProgress(
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> source
    ) {
        if (!source.status().carriesValue()) {
            return empty(source.status());
        }
        if (source.value().totalQuantity() == 0L) {
            return empty(SourceStatus.N_A);
        }
        return observed((double) source.value().activeCount() / source.value().totalQuantity(), source);
    }

    /** Prometheus가 계산한 rate 표본에서 현재 및 요청 구간 최고 발급률을 계산합니다. */
    private static Observation<RateSummary> issuanceRate(
            CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> source,
            Instant windowStart,
            Instant snapshotAt
    ) {
        if (!source.status().carriesValue()) {
            return empty(source.status());
        }
        List<CouponMetricsSource.IssuanceRateSample> samples = source.value();
        validateRateSampleRange(samples, windowStart, snapshotAt);
        if (source.status() == SourceStatus.NO_TRAFFIC) {
            validateNoTrafficRateSamples(samples);
            return observed(new RateSummary(0.0, 0.0), source);
        }
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("발급률을 계산할 요청 구간 표본이 없습니다.");
        }
        // 완전성 상태는 Reader가 판단하고 계산기는 확보된 rate 표본만 요약합니다.
        double current = samples.getLast().perSecond();
        double peak = samples.stream()
                .filter(sample -> !sample.observedAt().isBefore(windowStart))
                .mapToDouble(CouponMetricsSource.IssuanceRateSample::perSecond)
                .max()
                .orElseThrow(() -> new IllegalArgumentException(
                        "발급률을 계산할 요청 구간 표본이 없습니다."));
        return observed(new RateSummary(current, peak), source);
    }

    /** 입장 처리량과 현재 대기 수로 예상 대기시간을 계산합니다. */
    private static QueueSummary queue(
            CouponMetricsSource.Observation<CouponMetricsSource.QueueCounts> source
    ) {
        if (!source.status().carriesValue()) {
            return new QueueSummary(empty(source.status()), empty(source.status()));
        }

        CouponMetricsSource.QueueCounts value = source.value();
        Observation<Long> waiting = observed(value.waitingCount(), source);
        if (value.waitingCount() == 0L) {
            return new QueueSummary(waiting, observed(Duration.ZERO, source));
        }
        if (value.admittedCount() == 0L) {
            // 대기자가 있는데 입장이 멈춘 경우 무한대 대신 계산 불가를 명시합니다.
            return new QueueSummary(waiting, empty(SourceStatus.N_A));
        }

        double intervalSeconds = durationSeconds(value.windowStart(), value.windowEnd());
        double waitSeconds = value.waitingCount() * intervalSeconds / value.admittedCount();
        Duration estimatedWait = Duration.ofNanos(Math.round(waitSeconds * 1_000_000_000.0));
        return new QueueSummary(waiting, observed(estimatedWait, source));
    }

    /** 현재 발급·사용 보유량 중 사용 상태의 비율을 계산합니다. */
    private static Observation<Double> usageRatio(
            CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> source
    ) {
        if (!source.status().carriesValue()) {
            return empty(source.status());
        }
        long denominator = Math.addExact(source.value().issued(), source.value().used());
        if (denominator == 0L) {
            return empty(SourceStatus.N_A);
        }
        return observed((double) source.value().used() / denominator, source);
    }

    /** 원천의 상태별 보유량을 결과 계약으로 변환합니다. */
    private static Observation<IssuanceStatusCounts> holdingCounts(
            CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> source
    ) {
        if (!source.status().carriesValue()) {
            return empty(source.status());
        }
        CouponMetricsSource.IssuanceStatusCounts value = source.value();
        return observed(new IssuanceStatusCounts(
                value.issued(), value.used(), value.cancelled(), value.expired()), source);
    }

    /** 요청 구간과 겹치는 버킷의 상태별 초당 전이율을 계산합니다. */
    private static Observation<TransitionRateSummary> transitionRates(
            CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> source,
            Instant windowStart,
            Instant snapshotAt
    ) {
        if (!source.status().carriesValue()) {
            return empty(source.status());
        }
        if (source.status() == SourceStatus.NO_TRAFFIC) {
            validateNoTrafficTransitions(source.value());
            return observed(new TransitionRateSummary(0.0, 0.0, 0.0, 0.0), source);
        }

        long use = 0L;
        long cancelUse = 0L;
        long cancel = 0L;
        long expire = 0L;
        double overlapSeconds = 0.0;
        for (CouponMetricsSource.TransitionBucket bucket : source.value()) {
            Instant overlapStart = bucket.windowStart().isAfter(windowStart)
                    ? bucket.windowStart() : windowStart;
            Instant overlapEnd = bucket.windowEnd().isBefore(snapshotAt)
                    ? bucket.windowEnd() : snapshotAt;
            if (!overlapEnd.isAfter(overlapStart)) {
                continue;
            }
            if (!overlapStart.equals(bucket.windowStart())
                    || !overlapEnd.equals(bucket.windowEnd())) {
                throw new IllegalArgumentException(
                        "요청 구간과 부분적으로 겹치는 상태 전이 버킷은 계산할 수 없습니다.");
            }
            use = Math.addExact(use, bucket.use());
            cancelUse = Math.addExact(cancelUse, bucket.cancelUse());
            cancel = Math.addExact(cancel, bucket.cancel());
            expire = Math.addExact(expire, bucket.expire());
            overlapSeconds += durationSeconds(overlapStart, overlapEnd);
        }
        if (overlapSeconds == 0.0) {
            return empty(SourceStatus.N_A);
        }
        return observed(new TransitionRateSummary(
                use / overlapSeconds,
                cancelUse / overlapSeconds,
                cancel / overlapSeconds,
                expire / overlapSeconds), source);
    }

    /** 활성 재고와 발급·사용 상태 보유량이 같은 모집단인지 확인합니다. */
    private static void validateHoldingConsistency(
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
            CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdings
    ) {
        if (!stock.status().carriesValue() || !holdings.status().carriesValue()) {
            return;
        }
        long activeHoldings = Math.addExact(holdings.value().issued(), holdings.value().used());
        if (activeHoldings != stock.value().activeCount()) {
            throw new IllegalArgumentException("issued + used는 stock.activeCount와 같아야 합니다.");
        }
    }

    /** NO_TRAFFIC 발급률 표본에 양수 발급률이 섞이지 않았는지 확인합니다. */
    private static void validateNoTrafficRateSamples(
            List<CouponMetricsSource.IssuanceRateSample> samples
    ) {
        boolean hasActivity = samples.stream().anyMatch(sample -> sample.perSecond() > 0.0);
        if (hasActivity) {
            throw new IllegalArgumentException("NO_TRAFFIC 발급 원천에는 양수 발급률이 없어야 합니다.");
        }
    }

    /** 요청 구간 밖이나 기준 시각 이후의 발급률 표본을 거부합니다. */
    private static void validateRateSampleRange(
            List<CouponMetricsSource.IssuanceRateSample> samples,
            Instant windowStart,
            Instant snapshotAt
    ) {
        boolean hasOutsideSample = samples.stream().anyMatch(sample ->
                sample.observedAt().isBefore(windowStart) || sample.observedAt().isAfter(snapshotAt));
        if (hasOutsideSample) {
            throw new IllegalArgumentException("발급률 표본은 요청 구간 안에 있어야 합니다.");
        }
    }

    /** NO_TRAFFIC 전이 버킷에 실제 상태 전이 건수가 섞이지 않았는지 확인합니다. */
    private static void validateNoTrafficTransitions(
            List<CouponMetricsSource.TransitionBucket> buckets
    ) {
        boolean hasActivity = buckets.stream().anyMatch(bucket -> bucket.use() != 0L
                || bucket.cancelUse() != 0L
                || bucket.cancel() != 0L
                || bucket.expire() != 0L);
        if (hasActivity) {
            throw new IllegalArgumentException("NO_TRAFFIC 전이 원천에는 상태 전이 건수가 없어야 합니다.");
        }
    }

    /** 두 시각 사이의 나노초 정밀 경과시간을 초 단위 실수로 반환합니다. */
    private static double durationSeconds(Instant start, Instant end) {
        Duration duration = Duration.between(start, end);
        return duration.getSeconds() + duration.getNano() / 1_000_000_000.0;
    }

    /** 파생값에 원천 상태와 실제 관측 시각을 그대로 전파합니다. */
    private static <T, S> Observation<T> observed(
            T value,
            CouponMetricsSource.Observation<S> source
    ) {
        return new Observation<>(value, source.status(), source.observedAt());
    }

    /** 값을 실을 수 없는 상태의 canonical 결과를 생성합니다. */
    private static <T> Observation<T> empty(SourceStatus status) {
        return new Observation<>(null, status, null);
    }
}

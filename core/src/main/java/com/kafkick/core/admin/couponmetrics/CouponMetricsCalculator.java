package com.kafkick.core.admin.couponmetrics;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.CampaignRuntimeSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.IssuanceStatusCounts;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.Observation;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.QueueSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.RateSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.StockSummary;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSnapshot.TransitionRateSummary;
import com.kafkick.core.observation.SourceStatus;

/**
 * 캠페인 상세 원천값을 요청 구간에 맞는 재고·발급·대기·사용 지표로 계산합니다.
 *
 * <p>외부 저장소나 HTTP 타입에 의존하지 않는 순수 계산기입니다. 원천이 값을 싣지 않는 상태면
 * 계산을 시도하지 않고 같은 상태를 결과에 보존합니다.</p>
 */
@Component
public class CouponMetricsCalculator {

    /**
     * 한 캠페인의 상세 지표 스냅샷을 계산합니다.
     *
     * @param source 상태와 관측 시각을 포함한 캠페인 원천값
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
                issuanceRate(source.issuanceSamples(), windowStart, snapshotAt),
                queue(source.queue()),
                new CampaignRuntimeSummary(source.campaign().status(), source.campaign().openedAt()),
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

    /** 누적 Counter 표본에서 현재 및 요청 구간 최고 발급률을 계산합니다. */
    private static Observation<RateSummary> issuanceRate(
            CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceCounterSample>> source,
            Instant windowStart,
            Instant snapshotAt
    ) {
        if (!source.status().carriesValue()) {
            return empty(source.status());
        }
        List<CouponMetricsSource.IssuanceCounterSample> samples = source.value();
        validateNoFutureSamples(samples, snapshotAt);
        if (source.status() == SourceStatus.NO_TRAFFIC) {
            validateNoTrafficSamples(samples);
            return observed(new RateSummary(0.0, 0.0), source);
        }

        if (samples.size() < 2) {
            throw new IllegalArgumentException("발급률 계산에는 두 개 이상의 Counter 표본이 필요합니다.");
        }
        CouponMetricsSource.IssuanceCounterSample first = samples.getFirst();
        CouponMetricsSource.IssuanceCounterSample last = samples.getLast();
        // 부분 시계열을 정상값처럼 반환하지 않도록 요청 구간 양 끝을 모두 덮는지 확인합니다.
        boolean hasWindowStart = samples.stream()
                .anyMatch(sample -> sample.observedAt().equals(windowStart));
        if (first.observedAt().isAfter(windowStart)
                || !hasWindowStart
                || !last.observedAt().equals(snapshotAt)) {
            throw new IllegalArgumentException("발급 Counter 표본이 요청 구간 시작과 종료를 모두 덮지 않습니다.");
        }

        double current = rate(samples.get(samples.size() - 2), last);
        double peak = 0.0;
        boolean intervalFound = false;
        for (int index = 1; index < samples.size(); index++) {
            CouponMetricsSource.IssuanceCounterSample previous = samples.get(index - 1);
            CouponMetricsSource.IssuanceCounterSample currentSample = samples.get(index);
            if (currentSample.observedAt().isAfter(windowStart)
                    && !currentSample.observedAt().isAfter(snapshotAt)) {
                peak = Math.max(peak, rate(previous, currentSample));
                intervalFound = true;
            }
        }
        if (!intervalFound) {
            throw new IllegalArgumentException("발급률을 계산할 요청 구간 표본이 없습니다.");
        }
        return observed(new RateSummary(current, peak), source);
    }

    /** 인접한 두 누적 Counter 표본의 실제 경과시간 기준 초당 증가량을 계산합니다. */
    private static double rate(
            CouponMetricsSource.IssuanceCounterSample previous,
            CouponMetricsSource.IssuanceCounterSample current
    ) {
        double elapsedSeconds = durationSeconds(previous.observedAt(), current.observedAt());
        long completed = Math.subtractExact(
                current.cumulativeCompletedCount(), previous.cumulativeCompletedCount());
        return completed / elapsedSeconds;
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

    /** NO_TRAFFIC 발급 표본에 실제 Counter 증가가 섞이지 않았는지 확인합니다. */
    private static void validateNoTrafficSamples(
            List<CouponMetricsSource.IssuanceCounterSample> samples
    ) {
        for (int index = 1; index < samples.size(); index++) {
            if (samples.get(index).cumulativeCompletedCount()
                    != samples.get(index - 1).cumulativeCompletedCount()) {
                throw new IllegalArgumentException("NO_TRAFFIC 발급 원천에는 Counter 증가가 없어야 합니다.");
            }
        }
    }

    /** 관측 상태와 무관하게 snapshotAt 이후의 Counter 표본을 거부합니다. */
    private static void validateNoFutureSamples(
            List<CouponMetricsSource.IssuanceCounterSample> samples,
            Instant snapshotAt
    ) {
        boolean hasFutureSample = samples.stream()
                .anyMatch(sample -> sample.observedAt().isAfter(snapshotAt));
        if (hasFutureSample) {
            throw new IllegalArgumentException("snapshotAt보다 미래인 발급 Counter 표본은 사용할 수 없습니다.");
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

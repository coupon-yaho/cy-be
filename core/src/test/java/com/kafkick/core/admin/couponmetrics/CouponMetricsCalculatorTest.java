package com.kafkick.core.admin.couponmetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

class CouponMetricsCalculatorTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");

    private final CouponMetricsCalculator calculator = new CouponMetricsCalculator();

    @Test
    void calculatesStockProgressUsageQueueAndWindowedRates() {
        CouponMetricsSnapshot result = calculator.calculate(
                sourceWithFifteenMinutes(), MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT);

        assertThat(result.stock().remainingCount().value()).isEqualTo(400L);
        assertThat(result.issuanceProgress().value()).isEqualTo(0.6);
        assertThat(result.issuanceRate().value().currentPerSecond()).isEqualTo(12.0);
        assertThat(result.issuanceRate().value().peakPerSecond()).isEqualTo(20.0);
        assertThat(result.queue().estimatedWait().value()).isEqualTo(Duration.ofSeconds(12));
        assertThat(result.usageRatio().value()).isEqualTo(0.25);
        assertThat(result.holdingCounts().value())
                .isEqualTo(new CouponMetricsSnapshot.IssuanceStatusCounts(450L, 150L, 20L, 10L));
    }

    @Test
    void appliesRequestedWindowToPeakAndTransitionRates() {
        CouponMetricsSource source = sourceWithFifteenMinutes();

        CouponMetricsSnapshot oneMinute = calculator.calculate(
                source, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);
        CouponMetricsSnapshot fiveMinutes = calculator.calculate(
                source, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT);
        CouponMetricsSnapshot fifteenMinutes = calculator.calculate(
                source, MetricsWindow.FIFTEEN_MINUTES, SNAPSHOT_AT);

        assertThat(oneMinute.issuanceRate().value().peakPerSecond()).isEqualTo(12.0);
        assertThat(fiveMinutes.issuanceRate().value().peakPerSecond()).isEqualTo(20.0);
        assertThat(fifteenMinutes.issuanceRate().value().peakPerSecond()).isEqualTo(30.0);
        assertThat(oneMinute.transitionRate().value().usePerSecond()).isEqualTo(1.0);
        assertThat(fiveMinutes.transitionRate().value().usePerSecond()).isEqualTo(0.6);
        assertThat(fifteenMinutes.transitionRate().value().usePerSecond()).isEqualTo(0.4);
    }

    @Test
    void rejectsTransitionBucketsThatPartiallyOverlapRequestedWindow() {
        CouponMetricsSource.TransitionBucket partialBucket =
                new CouponMetricsSource.TransitionBucket(
                        SNAPSHOT_AT.minus(Duration.ofMinutes(5)).minusSeconds(30),
                        SNAPSHOT_AT.minus(Duration.ofMinutes(4)).minusSeconds(30),
                        60L, 0L, 0L, 0L);
        CouponMetricsSource source = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                observed(issuanceSamples()),
                observed(List.of(partialBucket)));

        assertThatThrownBy(() -> calculator.calculate(
                source, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("부분");
    }

    @Test
    void returnsNotApplicableUsageWhenNoIssuedOrUsedCouponsExist() {
        CouponMetricsSource source = sourceWith(
                observed(new CouponMetricsSource.StockCounts(0L, 0L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(0L, 0L, 0L, 0L)),
                noTraffic(List.of()),
                observed(transitionBuckets()));

        CouponMetricsSnapshot result = calculator.calculate(
                source, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);

        assertThat(result.usageRatio().status()).isEqualTo(SourceStatus.N_A);
        assertThat(result.usageRatio().value()).isNull();
        assertThat(result.usageRatio().observedAt()).isNull();
        assertThat(result.issuanceProgress().status()).isEqualTo(SourceStatus.N_A);
    }

    @Test
    void distinguishesStoppedAdmissionsFromAnEmptyQueue() {
        CouponMetricsSource waiting = sourceWithQueue(new CouponMetricsSource.QueueCounts(
                10L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT));
        CouponMetricsSource empty = sourceWithQueue(new CouponMetricsSource.QueueCounts(
                0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT));

        CouponMetricsSnapshot waitingResult = calculator.calculate(
                waiting, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT);
        CouponMetricsSnapshot emptyResult = calculator.calculate(
                empty, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT);

        assertThat(waitingResult.queue().estimatedWait().status()).isEqualTo(SourceStatus.N_A);
        assertThat(waitingResult.queue().estimatedWait().value()).isNull();
        assertThat(emptyResult.queue().estimatedWait().status()).isEqualTo(SourceStatus.VALID);
        assertThat(emptyResult.queue().estimatedWait().value()).isEqualTo(Duration.ZERO);
    }

    @Test
    void preservesNoTrafficAsObservedZeroRate() {
        CouponMetricsSource source = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                noTraffic(List.of()),
                observed(transitionBuckets()));

        CouponMetricsSnapshot result = calculator.calculate(
                source, MetricsWindow.FIFTEEN_MINUTES, SNAPSHOT_AT);

        assertThat(result.issuanceRate().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(result.issuanceRate().observedAt()).isEqualTo(SNAPSHOT_AT);
        assertThat(result.issuanceRate().value())
                .isEqualTo(new CouponMetricsSnapshot.RateSummary(0.0, 0.0));
    }

    @Test
    void rejectsNoTrafficSourcesThatContainObservedActivity() {
        CouponMetricsSource activeCounter = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                noTraffic(issuanceSamples()),
                observed(transitionBuckets()));
        CouponMetricsSource activeTransitions = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                observed(issuanceSamples()),
                noTraffic(transitionBuckets()));

        assertThatThrownBy(() -> calculator.calculate(
                activeCounter, MetricsWindow.FIFTEEN_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NO_TRAFFIC");
        assertThatThrownBy(() -> calculator.calculate(
                activeTransitions, MetricsWindow.FIFTEEN_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("NO_TRAFFIC");
    }

    @Test
    void preservesUnavailableAreasWithoutInventingZero() {
        CouponMetricsSource source = sourceWith(
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable());

        CouponMetricsSnapshot result = calculator.calculate(
                source, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT);

        assertThat(result.stock().remainingCount().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.stock().remainingCount().value()).isNull();
        assertThat(result.issuanceRate().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.queue().estimatedWait().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.usageRatio().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.transitionRate().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    void rejectsHoldingCountsThatDoNotMatchActiveStock() {
        CouponMetricsSource source = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(449L, 150L, 20L, 10L)),
                observed(issuanceSamples()),
                observed(transitionBuckets()));

        assertThatThrownBy(() -> calculator.calculate(
                source, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("activeCount");
    }

    @Test
    void rejectsFutureIssuanceSamples() {
        List<CouponMetricsSource.IssuanceCounterSample> samples = new ArrayList<>(issuanceSamples());
        samples.add(new CouponMetricsSource.IssuanceCounterSample(SNAPSHOT_AT.plusSeconds(1), 20_000L));
        CouponMetricsSource source = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                observed(samples),
                observed(transitionBuckets()));

        assertThatThrownBy(() -> calculator.calculate(
                source, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("미래");

        CouponMetricsSource noTrafficWithFuture = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                noTraffic(List.of(
                        new CouponMetricsSource.IssuanceCounterSample(SNAPSHOT_AT, 10L),
                        new CouponMetricsSource.IssuanceCounterSample(SNAPSHOT_AT.plusSeconds(1), 10L))),
                observed(transitionBuckets()));

        assertThatThrownBy(() -> calculator.calculate(
                noTrafficWithFuture, MetricsWindow.FIVE_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("미래");
    }

    @Test
    void rejectsPartialSamplesThatDoNotCoverRequestedWindow() {
        List<CouponMetricsSource.IssuanceCounterSample> samples = issuanceSamples().stream()
                .filter(sample -> !sample.observedAt().isBefore(SNAPSHOT_AT.minus(Duration.ofMinutes(5))))
                .toList();
        CouponMetricsSource source = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                observed(samples),
                observed(transitionBuckets()));

        assertThatThrownBy(() -> calculator.calculate(
                source, MetricsWindow.FIFTEEN_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("요청 구간");
    }

    @Test
    void rejectsSamplesWithoutAnExactCounterAtWindowStart() {
        List<CouponMetricsSource.IssuanceCounterSample> samples = new ArrayList<>();
        samples.add(sample(-16, 0L));
        samples.addAll(issuanceSamples().subList(1, issuanceSamples().size()));
        CouponMetricsSource source = sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        0L, 0L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                observed(samples),
                observed(transitionBuckets()));

        assertThatThrownBy(() -> calculator.calculate(
                source, MetricsWindow.FIFTEEN_MINUTES, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("시작");
    }

    private static CouponMetricsSource sourceWithFifteenMinutes() {
        return sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(new CouponMetricsSource.QueueCounts(
                        10L, 50L, SNAPSHOT_AT.minusSeconds(60), SNAPSHOT_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                observed(issuanceSamples()),
                observed(transitionBuckets()));
    }

    private static CouponMetricsSource sourceWithQueue(CouponMetricsSource.QueueCounts queue) {
        return sourceWith(
                observed(new CouponMetricsSource.StockCounts(1_000L, 600L)),
                observed(queue),
                observed(new CouponMetricsSource.IssuanceStatusCounts(450L, 150L, 20L, 10L)),
                observed(issuanceSamples()),
                observed(transitionBuckets()));
    }

    private static CouponMetricsSource sourceWith(
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
            CouponMetricsSource.Observation<CouponMetricsSource.QueueCounts> queue,
            CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdings,
            CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceCounterSample>> samples,
            CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> transitions
    ) {
        return new CouponMetricsSource(
                101L,
                new CouponMetricsSource.CampaignRuntime(
                        CouponStatus.OPEN, SNAPSHOT_AT.minus(Duration.ofHours(1))),
                stock,
                samples,
                queue,
                holdings,
                transitions);
    }

    private static List<CouponMetricsSource.IssuanceCounterSample> issuanceSamples() {
        List<CouponMetricsSource.IssuanceCounterSample> samples = new ArrayList<>();
        long cumulative = 0L;
        samples.add(sample(-15, cumulative));
        for (int minute = -14; minute <= 0; minute++) {
            double rate = switch (minute) {
                case -13 -> 30.0;
                case -4 -> 20.0;
                case 0 -> 12.0;
                default -> 5.0;
            };
            cumulative += (long) (rate * 60L);
            samples.add(sample(minute, cumulative));
        }
        return samples;
    }

    private static CouponMetricsSource.IssuanceCounterSample sample(int minute, long cumulative) {
        return new CouponMetricsSource.IssuanceCounterSample(
                SNAPSHOT_AT.plus(Duration.ofMinutes(minute)), cumulative);
    }

    private static List<CouponMetricsSource.TransitionBucket> transitionBuckets() {
        List<CouponMetricsSource.TransitionBucket> buckets = new ArrayList<>();
        for (int minute = -15; minute < 0; minute++) {
            long use = minute == -1 ? 60L : minute >= -5 ? 30L : 18L;
            buckets.add(new CouponMetricsSource.TransitionBucket(
                    SNAPSHOT_AT.plus(Duration.ofMinutes(minute)),
                    SNAPSHOT_AT.plus(Duration.ofMinutes(minute + 1L)),
                    use, 6L, 3L, 1L));
        }
        return buckets;
    }

    private static <T> CouponMetricsSource.Observation<T> observed(T value) {
        return new CouponMetricsSource.Observation<>(value, SourceStatus.VALID, SNAPSHOT_AT);
    }

    private static <T> CouponMetricsSource.Observation<T> noTraffic(T value) {
        return new CouponMetricsSource.Observation<>(value, SourceStatus.NO_TRAFFIC, SNAPSHOT_AT);
    }

    private static <T> CouponMetricsSource.Observation<T> unavailable() {
        return new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }
}

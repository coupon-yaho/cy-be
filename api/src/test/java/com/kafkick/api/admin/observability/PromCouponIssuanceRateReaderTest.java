package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.observation.SourceStatus;

class PromCouponIssuanceRateReaderTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-25T00:00:00Z");
    private static final Duration STEP = Duration.ofSeconds(5);

    @Test
    void readsTheRequestedWindowAtFiveSecondSteps() {
        RecordingRangeQuery rangeQuery = new RecordingRangeQuery(query -> oneSeries(grid(2.0)));
        RecordingTimeQuery timeQuery = new RecordingTimeQuery((query, evaluationAt) -> freshnessAt(SNAPSHOT_AT));

        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> result = reader(
                rangeQuery, timeQuery).read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);

        assertThat(rangeQuery.calls).containsExactly(new RangeCall(
                "sum(rate(app_issuance_flow_total{coupon_id=\"10\",stage=\"success\"}[5s]))",
                SNAPSHOT_AT.minus(Duration.ofMinutes(1)), SNAPSHOT_AT, STEP));
        assertThat(timeQuery.calls).containsExactly(new TimeCall(
                "min(timestamp(app_issuance_flow_total{coupon_id=\"10\",stage=\"success\"}))",
                SNAPSHOT_AT));
        assertThat(result.status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.value()).hasSize(13);
    }

    @Test
    void distinguishesAbsentSeriesFromMeasuredZeroRates() {
        RecordingRangeQuery absentRange = new RecordingRangeQuery(query -> List.of());
        RecordingTimeQuery unusedTime = new RecordingTimeQuery((query, evaluationAt) -> {
            throw new AssertionError("빈 rate 시계열에는 freshness 질의를 보내면 안 됩니다.");
        });

        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> absent = reader(
                absentRange, unusedTime).read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);
        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> zero = reader(
                new RecordingRangeQuery(query -> oneSeries(grid(0.0))),
                new RecordingTimeQuery((query, evaluationAt) -> freshnessAt(SNAPSHOT_AT)))
                .read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);

        assertThat(absent.status()).isEqualTo(SourceStatus.PENDING);
        assertThat(absent.value()).isNull();
        assertThat(zero.status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(zero.value()).allSatisfy(sample -> assertThat(sample.perSecond()).isZero());
    }

    @Test
    void preservesIncompleteAndStaleRateSamplesWithTheirOwnStatuses() {
        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> warmingUp = reader(
                new RecordingRangeQuery(query -> oneSeries(List.of(
                        new PromRangePoint(SNAPSHOT_AT.minusSeconds(5), 1.0),
                        new PromRangePoint(SNAPSHOT_AT, 2.0)))),
                new RecordingTimeQuery((query, evaluationAt) -> freshnessAt(SNAPSHOT_AT)))
                .read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);
        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> stale = reader(
                new RecordingRangeQuery(query -> oneSeries(grid(2.0))),
                new RecordingTimeQuery((query, evaluationAt) -> freshnessAt(SNAPSHOT_AT.minusSeconds(121))))
                .read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);

        assertThat(warmingUp.status()).isEqualTo(SourceStatus.WARMING_UP);
        assertThat(warmingUp.value()).hasSize(2);
        assertThat(stale.status()).isEqualTo(SourceStatus.STALE);
        assertThat(stale.value()).hasSize(13);
        assertThat(stale.observedAt()).isEqualTo(SNAPSHOT_AT.minusSeconds(121));
    }

    @Test
    void classifiesAnInterruptedStaleRateGridAsStaleBeforeWarmingUp() {
        Instant staleAt = SNAPSHOT_AT.minusSeconds(125);

        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> result = reader(
                new RecordingRangeQuery(query -> oneSeries(gridUntil(
                        MetricsWindow.FIFTEEN_MINUTES, staleAt, 2.0))),
                new RecordingTimeQuery((query, evaluationAt) -> freshnessAt(staleAt)))
                .read(10L, MetricsWindow.FIFTEEN_MINUTES, SNAPSHOT_AT);

        assertThat(result.status()).isEqualTo(SourceStatus.STALE);
        assertThat(result.value()).hasSize(156);
        assertThat(result.value().getLast().observedAt()).isEqualTo(staleAt);
        assertThat(result.observedAt()).isEqualTo(staleAt);
    }

    @Test
    void skipsFreshnessWhenTheRangeQueryConsumesTheSeriesBudget() {
        MutableNanoTime nanoTime = new MutableNanoTime();
        PrometheusSeriesProperties properties = new PrometheusSeriesProperties(
                Duration.ofMillis(1), Duration.ofMillis(1), Duration.ofSeconds(2), STEP, 300);
        RecordingRangeQuery rangeQuery = new RecordingRangeQuery(query -> {
            nanoTime.advance(Duration.ofMillis(2_001));
            return oneSeries(grid(2.0));
        });
        RecordingTimeQuery timeQuery = new RecordingTimeQuery((query, evaluationAt) -> {
            throw new AssertionError("소진된 예산에서는 freshness 질의를 보내면 안 됩니다.");
        });

        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> result = reader(
                rangeQuery, timeQuery, properties, nanoTime).read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);

        assertThat(result.status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(timeQuery.calls).isEmpty();
    }

    @Test
    void isolatesQueryAndResponseShapeFailuresAsUnavailable() {
        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> queryFailure = reader(
                new RecordingRangeQuery(query -> {
                    throw new PromQueryException("range down");
                }), new RecordingTimeQuery((query, evaluationAt) -> freshnessAt(SNAPSHOT_AT)))
                .read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);
        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> multipleSeries = reader(
                new RecordingRangeQuery(query -> List.of(
                        new PromRangeSeries(Map.of(), grid(1.0)),
                        new PromRangeSeries(Map.of(), grid(1.0)))),
                new RecordingTimeQuery((query, evaluationAt) -> freshnessAt(SNAPSHOT_AT)))
                .read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);
        CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> malformedFreshness = reader(
                new RecordingRangeQuery(query -> oneSeries(grid(1.0))),
                new RecordingTimeQuery((query, evaluationAt) -> List.of(
                        new PromSample("timestamp", Map.of(), Double.NaN, SNAPSHOT_AT))))
                .read(10L, MetricsWindow.ONE_MINUTE, SNAPSHOT_AT);

        assertThat(queryFailure.status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(multipleSeries.status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(malformedFreshness.status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    private static PromCouponIssuanceRateReader reader(
            PromRangeQuery rangeQuery,
            PromTimeQuery timeQuery
    ) {
        return reader(rangeQuery, timeQuery, PrometheusSeriesProperties.defaults(), System::nanoTime);
    }

    private static PromCouponIssuanceRateReader reader(
            PromRangeQuery rangeQuery,
            PromTimeQuery timeQuery,
            PrometheusSeriesProperties properties,
            LongSupplier nanoTime
    ) {
        return new PromCouponIssuanceRateReader(
                rangeQuery, timeQuery, properties, Duration.ofSeconds(120), nanoTime);
    }

    private static List<PromRangeSeries> oneSeries(List<PromRangePoint> points) {
        return List.of(new PromRangeSeries(Map.of(), points));
    }

    private static List<PromRangePoint> grid(double value) {
        List<PromRangePoint> points = new ArrayList<>();
        for (int offset = 60; offset >= 0; offset -= 5) {
            points.add(new PromRangePoint(SNAPSHOT_AT.minusSeconds(offset), value));
        }
        return List.copyOf(points);
    }

    private static List<PromRangePoint> gridUntil(MetricsWindow window, Instant lastObservedAt, double value) {
        List<PromRangePoint> points = new ArrayList<>();
        Instant sampleAt = SNAPSHOT_AT.minus(window.duration());
        while (!sampleAt.isAfter(lastObservedAt)) {
            points.add(new PromRangePoint(sampleAt, value));
            sampleAt = sampleAt.plus(STEP);
        }
        return List.copyOf(points);
    }

    private static List<PromSample> freshnessAt(Instant observedAt) {
        return List.of(new PromSample("timestamp", Map.of(), observedAt.toEpochMilli() / 1000.0, SNAPSHOT_AT));
    }

    private static final class RecordingRangeQuery implements PromRangeQuery {
        private final Function<String, List<PromRangeSeries>> responder;
        private final List<RangeCall> calls = new ArrayList<>();

        private RecordingRangeQuery(Function<String, List<PromRangeSeries>> responder) {
            this.responder = responder;
        }

        @Override
        public List<PromRangeSeries> query(String promQl, Instant start, Instant end, Duration step) {
            calls.add(new RangeCall(promQl, start, end, step));
            return responder.apply(promQl);
        }
    }

    private static final class RecordingTimeQuery implements PromTimeQuery {
        private final BiFunction<String, Instant, List<PromSample>> responder;
        private final List<TimeCall> calls = new ArrayList<>();

        private RecordingTimeQuery(BiFunction<String, Instant, List<PromSample>> responder) {
            this.responder = responder;
        }

        @Override
        public List<PromSample> query(String promQl, Instant evaluationAt) {
            calls.add(new TimeCall(promQl, evaluationAt));
            return responder.apply(promQl, evaluationAt);
        }
    }

    private record RangeCall(String promQl, Instant start, Instant end, Duration step) { }

    private record TimeCall(String promQl, Instant evaluationAt) { }

    private static final class MutableNanoTime implements LongSupplier {

        private long currentNanos;

        @Override
        public long getAsLong() {
            return currentNanos;
        }

        private void advance(Duration duration) {
            currentNanos += duration.toNanos();
        }
    }
}

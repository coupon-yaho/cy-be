package com.kafkick.core.admin.couponmetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

class CouponMetricsSourceTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-22T00:00:00Z");

    @Test
    void unavailableSourceCannotCarryValueOrObservedAt() {
        assertThatThrownBy(() -> new CouponMetricsSource.Observation<>(
                new CouponMetricsSource.StockCounts(100L, 40L),
                SourceStatus.UNAVAILABLE,
                OBSERVED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void validSourceRequiresValueAndObservedAt() {
        assertThatThrownBy(() -> new CouponMetricsSource.Observation<>(
                null,
                SourceStatus.VALID,
                null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeOrNonFiniteRateSamples() {
        assertThatThrownBy(() -> new CouponMetricsSource.IssuanceRateSample(OBSERVED_AT, -0.1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.IssuanceRateSample(OBSERVED_AT, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.IssuanceRateSample(
                OBSERVED_AT, Double.POSITIVE_INFINITY))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvalidCouponAndCountRelationships() {
        CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock = observed(
                new CouponMetricsSource.StockCounts(100L, 40L));

        assertThatThrownBy(() -> source(0L, stock, observed(List.of()), observed(List.of())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.StockCounts(100L, 101L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.IssuanceStatusCounts(-1L, 0L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.IssuanceStatusCounts(0L, -1L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.IssuanceStatusCounts(0L, 0L, -1L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.IssuanceStatusCounts(0L, 0L, 0L, -1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.TransitionBucket(
                OBSERVED_AT.minusSeconds(1), OBSERVED_AT, -1L, 0L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.TransitionBucket(
                OBSERVED_AT.minusSeconds(1), OBSERVED_AT, 0L, -1L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.TransitionBucket(
                OBSERVED_AT.minusSeconds(1), OBSERVED_AT, 0L, 0L, -1L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponMetricsSource.TransitionBucket(
                OBSERVED_AT.minusSeconds(1), OBSERVED_AT, 0L, 0L, 0L, -1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOutOfOrderRatesAndOverlappingTransitionBuckets() {
        List<CouponMetricsSource.IssuanceRateSample> outOfOrder = List.of(
                new CouponMetricsSource.IssuanceRateSample(OBSERVED_AT, 10.0),
                new CouponMetricsSource.IssuanceRateSample(OBSERVED_AT.minusSeconds(10), 9.0));
        List<CouponMetricsSource.TransitionBucket> overlapping = List.of(
                bucket(OBSERVED_AT.minusSeconds(20), OBSERVED_AT.minusSeconds(10)),
                bucket(OBSERVED_AT.minusSeconds(15), OBSERVED_AT));

        assertThatThrownBy(() -> source(1L, observed(new CouponMetricsSource.StockCounts(100L, 40L)),
                observed(outOfOrder), observed(List.of())))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> source(1L, observed(new CouponMetricsSource.StockCounts(100L, 40L)),
                observed(List.of()), observed(overlapping)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void copiesSourceListsDefensively() {
        ArrayList<CouponMetricsSource.IssuanceRateSample> samples = new ArrayList<>();
        samples.add(new CouponMetricsSource.IssuanceRateSample(OBSERVED_AT, 10.0));

        CouponMetricsSource source = source(1L,
                observed(new CouponMetricsSource.StockCounts(100L, 40L)),
                observed(samples),
                observed(List.of()));
        samples.clear();

        assertThat(source.issuanceRateSamples().value()).hasSize(1);
        assertThatThrownBy(() -> source.issuanceRateSamples().value().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static CouponMetricsSource source(
            long couponId,
            CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock,
            CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> samples,
            CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> transitions
    ) {
        return new CouponMetricsSource(
                couponId,
                new CouponMetricsSource.CampaignRuntime(CouponRoundStatus.OPEN, OBSERVED_AT.minusSeconds(60)),
                stock,
                samples,
                observed(new CouponMetricsSource.QueueCounts(
                        10L, 5L, OBSERVED_AT.minusSeconds(10), OBSERVED_AT)),
                observed(new CouponMetricsSource.IssuanceStatusCounts(30L, 10L, 0L, 0L)),
                transitions);
    }

    private static CouponMetricsSource.TransitionBucket bucket(Instant start, Instant end) {
        return new CouponMetricsSource.TransitionBucket(start, end, 1L, 1L, 1L, 1L);
    }

    private static <T> CouponMetricsSource.Observation<T> observed(T value) {
        return new CouponMetricsSource.Observation<>(value, SourceStatus.VALID, OBSERVED_AT);
    }

}

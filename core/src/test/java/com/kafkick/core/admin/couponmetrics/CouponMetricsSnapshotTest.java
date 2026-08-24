package com.kafkick.core.admin.couponmetrics;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

class CouponMetricsSnapshotTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-22T00:00:00Z");

    @Test
    void rejectsRatiosOutsideZeroToOne() {
        CouponMetricsSnapshot.Observation<Double> invalidRatio = observed(1.1);

        assertThatThrownBy(() -> snapshot(invalidRatio, observed(0.25)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("issuanceProgress");
        assertThatThrownBy(() -> snapshot(observed(0.6), invalidRatio))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("usageRatio");
    }

    private static CouponMetricsSnapshot snapshot(
            CouponMetricsSnapshot.Observation<Double> issuanceProgress,
            CouponMetricsSnapshot.Observation<Double> usageRatio
    ) {
        CouponMetricsSnapshot.Observation<Long> count = observed(1L);
        return new CouponMetricsSnapshot(
                1L,
                OBSERVED_AT,
                MetricsWindow.ONE_MINUTE,
                new CouponMetricsSnapshot.StockSummary(count, count),
                issuanceProgress,
                observed(new CouponMetricsSnapshot.RateSummary(1.0, 1.0)),
                new CouponMetricsSnapshot.QueueSummary(count, observed(Duration.ZERO)),
                new CouponMetricsSnapshot.CampaignRuntimeSummary(CouponRoundStatus.OPEN, OBSERVED_AT),
                usageRatio,
                observed(new CouponMetricsSnapshot.IssuanceStatusCounts(1L, 0L, 0L, 0L)),
                observed(new CouponMetricsSnapshot.TransitionRateSummary(1.0, 1.0, 1.0, 1.0)));
    }

    private static <T> CouponMetricsSnapshot.Observation<T> observed(T value) {
        return new CouponMetricsSnapshot.Observation<>(value, SourceStatus.VALID, OBSERVED_AT);
    }
}

package com.kafkick.core.admin.stock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDetailData;
import com.kafkick.core.admin.couponroundsource.DetailAvailability;
import com.kafkick.core.admin.couponroundsource.PreparationSource;
import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

class AdminStockResolverTest {
    private static final Instant NOW = Instant.parse("2026-08-29T09:00:00Z");

    @Test
    void keepsV1DatabaseStockAndReplacesOnlyV2WithRedisAuthority() {
        AtomicInteger calls = new AtomicInteger();
        V2AdminStockReader reader = (requests, observedAt) -> {
            calls.incrementAndGet();
            assertThat(requests).extracting(V2AdminStockReader.Request::couponId).containsExactly(2L);
            return Map.of(2L, observed(new AdminStockSnapshot(100L, 30L)));
        };
        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, List.of(
                couponRound(1L, EngineVersion.V1, 100L, 20L),
                couponRound(2L, EngineVersion.V2, 100L, 20L)));

        AdminCouponRoundCatalog resolved = new AdminStockResolver(reader).resolve(catalog, NOW);

        assertThat(calls).hasValue(1);
        assertThat(resolved.couponRounds().get(0).stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(100L, 20L));
        assertThat(resolved.couponRounds().get(1).stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(100L, 70L));
    }

    @Test
    void replacesV2DetailStockWithoutChangingDatabaseHoldingMetrics() {
        V2AdminStockReader reader = (requests, observedAt) -> Map.of(
                2L, observed(new AdminStockSnapshot(100L, 30L)));
        CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdings =
                observed(new CouponMetricsSource.IssuanceStatusCounts(1L, 2L, 3L, 4L));
        AdminCouponRoundDetailData detail = new AdminCouponRoundDetailData(DetailAvailability.AVAILABLE,
                new AdminCouponRoundDetailData.DetailValue(
                        2L, "couponRound", "brand", EngineVersion.V2,
                        new CouponMetricsSource.CouponRoundRuntime(CouponRoundStatus.OPEN, NOW.minusSeconds(60)),
                        observed(new CouponMetricsSource.StockCounts(100L, 20L)), holdings,
                        observed(List.of())));

        AdminCouponRoundDetailData resolved = new AdminStockResolver(reader).resolve(detail, NOW);

        assertThat(resolved.value().stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(100L, 70L));
        assertThat(resolved.value().holdingCounts()).isSameAs(holdings);
    }

    private static AdminCouponRoundCatalog.CouponRoundData couponRound(
            long id, EngineVersion engine, long total, long active) {
        return new AdminCouponRoundCatalog.CouponRoundData(
                id, "couponRound", "brand", engine, CouponRoundStatus.OPEN,
                NOW.minusSeconds(60), NOW.plusSeconds(60),
                observed(new CouponMetricsSource.StockCounts(total, active)),
                new PreparationSource(
                        true, true, CouponPolicyType.FIXED_AMOUNT, 3, SourceStatus.VALID, NOW));
    }

    private static <T> CouponMetricsSource.Observation<T> observed(T value) {
        return new CouponMetricsSource.Observation<>(value, SourceStatus.VALID, NOW);
    }
}

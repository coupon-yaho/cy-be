package com.kafkick.core.admin.stock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;
import com.kafkick.core.admin.campaignsource.PreparationSource;
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
        AdminCampaignCatalog catalog = new AdminCampaignCatalog(SourceStatus.VALID, NOW, List.of(
                campaign(1L, EngineVersion.V1, 100L, 20L),
                campaign(2L, EngineVersion.V2, 100L, 20L)));

        AdminCampaignCatalog resolved = new AdminStockResolver(reader).resolve(catalog, NOW);

        assertThat(calls).hasValue(1);
        assertThat(resolved.campaigns().get(0).stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(100L, 20L));
        assertThat(resolved.campaigns().get(1).stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(100L, 70L));
    }

    @Test
    void replacesV2DetailStockWithoutChangingDatabaseHoldingMetrics() {
        V2AdminStockReader reader = (requests, observedAt) -> Map.of(
                2L, observed(new AdminStockSnapshot(100L, 30L)));
        CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdings =
                observed(new CouponMetricsSource.IssuanceStatusCounts(1L, 2L, 3L, 4L));
        AdminCampaignDetailData detail = new AdminCampaignDetailData(DetailAvailability.AVAILABLE,
                new AdminCampaignDetailData.DetailValue(
                        2L, "campaign", "brand", EngineVersion.V2,
                        new CouponMetricsSource.CampaignRuntime(CouponRoundStatus.OPEN, NOW.minusSeconds(60)),
                        observed(new CouponMetricsSource.StockCounts(100L, 20L)), holdings,
                        observed(List.of())));

        AdminCampaignDetailData resolved = new AdminStockResolver(reader).resolve(detail, NOW);

        assertThat(resolved.value().stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(100L, 70L));
        assertThat(resolved.value().holdingCounts()).isSameAs(holdings);
    }

    private static AdminCampaignCatalog.CampaignData campaign(
            long id, EngineVersion engine, long total, long active) {
        return new AdminCampaignCatalog.CampaignData(
                id, "campaign", "brand", engine, CouponRoundStatus.OPEN,
                NOW.minusSeconds(60), NOW.plusSeconds(60),
                observed(new CouponMetricsSource.StockCounts(total, active)),
                new PreparationSource(
                        true, true, CouponPolicyType.FIXED_AMOUNT, 3, SourceStatus.VALID, NOW));
    }

    private static <T> CouponMetricsSource.Observation<T> observed(T value) {
        return new CouponMetricsSource.Observation<>(value, SourceStatus.VALID, NOW);
    }
}

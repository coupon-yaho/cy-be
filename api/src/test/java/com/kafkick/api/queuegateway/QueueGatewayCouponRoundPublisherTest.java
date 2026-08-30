package com.kafkick.api.queuegateway;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.couponroundsource.PreparationSource;
import com.kafkick.core.admin.stock.AdminStockResolver;
import com.kafkick.core.admin.stock.AdminStockSnapshot;
import com.kafkick.core.admin.stock.V2AdminStockReader;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.queuegateway.QueueGatewayCouponRoundState;
import com.kafkick.core.queuegateway.QueueGatewayStatePort;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;

class QueueGatewayCouponRoundPublisherTest {

    private static final Instant NOW = Instant.parse("2026-08-31T02:00:00Z");

    @Test
    void publishesOnlyOpenRoundsUsingVersionAuthoritativeRemainingStock() {
        AdminCouponRoundDataReader reader = mock(AdminCouponRoundDataReader.class);
        when(reader.loadCatalog(NOW)).thenReturn(catalog(List.of(
                round(10L, EngineVersion.V1, CouponRoundStatus.OPEN, 100L, 30L),
                round(11L, EngineVersion.V2, CouponRoundStatus.OPEN, 100L, 80L),
                round(12L, EngineVersion.V1, CouponRoundStatus.CLOSED, 100L, 90L))));
        V2AdminStockReader v2Reader = (requests, observedAt) -> {
            LinkedHashMap<Long, CouponMetricsSource.Observation<AdminStockSnapshot>> result =
                    new LinkedHashMap<>();
            result.put(11L, new CouponMetricsSource.Observation<>(
                    new AdminStockSnapshot(100L, 25L), SourceStatus.VALID, observedAt));
            return result;
        };
        QueueGatewayStatePort port = mock(QueueGatewayStatePort.class);

        publisher(reader, new AdminStockResolver(v2Reader), runtimeConfig(QueueMode.ADAPTIVE), port)
                .publishCouponRounds();

        verify(reader).loadCatalog(NOW);
        verify(port).publishCouponRounds(List.of(
                new QueueGatewayCouponRoundState(10L, 70L, SourceStatus.VALID, NOW),
                new QueueGatewayCouponRoundState(11L, 25L, SourceStatus.VALID, NOW)),
                QueueMode.ADAPTIVE);
    }

    @Test
    void keepsAnOpenRoundButPublishesNoReplacementStockWhenV2IsUnavailable() {
        AdminCouponRoundDataReader reader = mock(AdminCouponRoundDataReader.class);
        when(reader.loadCatalog(NOW)).thenReturn(catalog(List.of(
                round(11L, EngineVersion.V2, CouponRoundStatus.OPEN, 100L, 80L))));
        QueueGatewayStatePort port = mock(QueueGatewayStatePort.class);

        publisher(reader, new AdminStockResolver(AdminStockResolver.unavailableV2Reader()),
                runtimeConfig(QueueMode.ALWAYS), port).publishCouponRounds();

        verify(port).publishCouponRounds(List.of(
                new QueueGatewayCouponRoundState(11L, null, SourceStatus.UNAVAILABLE, null)),
                QueueMode.ALWAYS);
    }

    @Test
    void unavailableCatalogOrSourceFailurePreservesThePreviousRedisSnapshot() {
        AdminCouponRoundDataReader unavailable = mock(AdminCouponRoundDataReader.class);
        when(unavailable.loadCatalog(NOW)).thenReturn(
                new AdminCouponRoundCatalog(SourceStatus.UNAVAILABLE, null, List.of()));
        QueueGatewayStatePort firstPort = mock(QueueGatewayStatePort.class);
        publisher(unavailable, new AdminStockResolver(AdminStockResolver.unavailableV2Reader()),
                runtimeConfig(QueueMode.OFF), firstPort).publishCouponRounds();
        verify(firstPort, never()).publishCouponRounds(any(), any());

        AdminCouponRoundDataReader failing = mock(AdminCouponRoundDataReader.class);
        when(failing.loadCatalog(NOW)).thenThrow(new IllegalStateException("db down"));
        QueueGatewayStatePort secondPort = mock(QueueGatewayStatePort.class);
        publisher(failing, new AdminStockResolver(AdminStockResolver.unavailableV2Reader()),
                runtimeConfig(QueueMode.OFF), secondPort).publishCouponRounds();
        verify(secondPort, never()).publishCouponRounds(any(), any());
    }

    private static QueueGatewayCouponRoundPublisher publisher(
            AdminCouponRoundDataReader reader,
            AdminStockResolver stockResolver,
            RuntimeConfigStore runtimeConfigStore,
            QueueGatewayStatePort port
    ) {
        QueueGatewayPublisherProperties properties = new QueueGatewayPublisherProperties(
                true, Duration.ofSeconds(1), Duration.ofSeconds(5), 100L, "api-1");
        return new QueueGatewayCouponRoundPublisher(
                reader, stockResolver, runtimeConfigStore, port, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static RuntimeConfigStore runtimeConfig(QueueMode queueMode) {
        RuntimeConfigStore store = mock(RuntimeConfigStore.class);
        when(store.get()).thenReturn(new RuntimeConfigSnapshot(
                EngineVersion.V2, ReleaseStage.V2_2, queueMode, 1L, NOW, "admin", SourceStatus.VALID));
        return store;
    }

    private static AdminCouponRoundCatalog catalog(List<AdminCouponRoundCatalog.CouponRoundData> rounds) {
        return new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, rounds);
    }

    private static AdminCouponRoundCatalog.CouponRoundData round(
            long couponId,
            EngineVersion version,
            CouponRoundStatus status,
            long total,
            long issued
    ) {
        return new AdminCouponRoundCatalog.CouponRoundData(
                couponId, "coupon-" + couponId, "brand", version, status,
                NOW.minusSeconds(60), NOW.plusSeconds(60),
                new CouponMetricsSource.Observation<>(
                        new CouponMetricsSource.StockCounts(total, issued), SourceStatus.VALID, NOW),
                new PreparationSource(true, true, CouponPolicyType.FIXED_AMOUNT, 1,
                        SourceStatus.VALID, NOW));
    }
}

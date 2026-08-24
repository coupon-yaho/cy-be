package com.kafkick.core.admin.couponmetrics.mock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataset;
import com.kafkick.core.observation.SourceStatus;

class AdminCouponMetricsMockDataFactoryTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");

    private final AdminOverviewMockDataFactory overviewFactory = new AdminOverviewMockDataFactory();
    private final AdminCouponMetricsMockDataFactory factory =
            new AdminCouponMetricsMockDataFactory(overviewFactory);

    @Test
    void usesExactlyTheSameCouponPopulationAsOverviewMock() {
        AdminOverviewMockDataset overview = overviewFactory.create(SNAPSHOT_AT);

        assertThat(overview.campaigns())
                .allSatisfy(campaign -> assertThat(factory.find(SNAPSHOT_AT, campaign.couponId()))
                        .isPresent());
        assertThat(factory.find(SNAPSHOT_AT, 999_999L)).isEmpty();
    }

    @Test
    void reusesOverviewStockAndQueueSourcesForTheSameCoupon() {
        AdminOverviewMockDataset overview = overviewFactory.create(SNAPSHOT_AT);
        CouponMetricsSource source = factory.find(SNAPSHOT_AT, 103L).orElseThrow();
        var campaign = overview.campaigns().stream()
                .filter(candidate -> candidate.couponId().equals(103L))
                .findFirst()
                .orElseThrow();
        var queue = overview.queueInputs().stream()
                .filter(candidate -> candidate.couponId().equals(103L))
                .findFirst()
                .orElseThrow();

        assertThat(source.stock().value())
                .isEqualTo(new CouponMetricsSource.StockCounts(
                        campaign.totalQuantity(), campaign.activeCount()));
        assertThat(source.stock().status()).isEqualTo(campaign.stockStatus());
        assertThat(source.stock().observedAt()).isEqualTo(campaign.stockObservedAt());
        assertThat(source.queue().value())
                .isEqualTo(new CouponMetricsSource.QueueCounts(
                        queue.currentWaitingCount(), queue.admittedCount(),
                        queue.windowStart(), queue.windowEnd()));
        assertThat(source.queue().status()).isEqualTo(queue.sourceStatus());
        assertThat(source.queue().observedAt()).isEqualTo(queue.observedAt());
    }

    @Test
    void createsFifteenMinutesOfRateAndTransitionSources() {
        CouponMetricsSource source = factory.find(SNAPSHOT_AT, 101L).orElseThrow();

        assertThat(source.issuanceSamples().value().getFirst().observedAt())
                .isEqualTo(SNAPSHOT_AT.minus(Duration.ofMinutes(15)));
        assertThat(source.issuanceSamples().value().getLast().observedAt())
                .isEqualTo(SNAPSHOT_AT);
        assertThat(source.issuanceSamples().value())
                .extracting(CouponMetricsSource.IssuanceCounterSample::cumulativeCompletedCount)
                .isSorted();
        assertThat(source.transitions().value()).hasSize(15);
        assertThat(source.transitions().value().getFirst().windowStart())
                .isEqualTo(SNAPSHOT_AT.minus(Duration.ofMinutes(15)));
        assertThat(source.transitions().value().getLast().windowEnd()).isEqualTo(SNAPSHOT_AT);
        assertThat(source.holdingCounts().value().issued()
                + source.holdingCounts().value().used())
                .isEqualTo(source.stock().value().activeCount());
    }

    @Test
    void marksScheduledAndClosedDetailSourcesAsNotApplicable() {
        CouponMetricsSource scheduled = factory.find(SNAPSHOT_AT, 104L).orElseThrow();
        CouponMetricsSource closed = factory.find(SNAPSHOT_AT, 106L).orElseThrow();

        assertNotApplicable(scheduled);
        assertNotApplicable(closed);
    }

    @Test
    void preservesStockFreshnessWhenCreatingHoldingCounts() {
        AdminOverviewMockDataset original = overviewFactory.create(SNAPSHOT_AT);
        CampaignOverviewSource campaign = original.campaigns().getFirst();
        Instant staleAt = SNAPSHOT_AT.minus(Duration.ofMinutes(2));
        CampaignOverviewSource staleCampaign = new CampaignOverviewSource(
                campaign.couponId(), campaign.campaignName(), campaign.brandName(), campaign.status(),
                campaign.opensAt(), campaign.closesAt(), campaign.engineVersion(),
                campaign.totalQuantity(), campaign.activeCount(), staleAt, SourceStatus.STALE,
                campaign.preparation());
        ArrayList<CampaignOverviewSource> campaigns = new ArrayList<>(original.campaigns());
        campaigns.set(0, staleCampaign);
        AdminOverviewMockDataset staleDataset = new AdminOverviewMockDataset(
                original.policy(), original.issuanceFlowInputs(), original.queueInputs(), original.outcomeInput(),
                campaigns, original.preparationActionCandidates(), original.consistencyActionContexts(),
                original.aggregateIssuanceRate(), original.latencySummary());
        AdminCouponMetricsMockDataFactory staleFactory = new AdminCouponMetricsMockDataFactory(
                new FixedOverviewFactory(staleDataset));

        CouponMetricsSource source = staleFactory.find(SNAPSHOT_AT, campaign.couponId()).orElseThrow();

        assertThat(source.holdingCounts().status()).isEqualTo(SourceStatus.STALE);
        assertThat(source.holdingCounts().observedAt()).isEqualTo(staleAt);
    }

    private static void assertNotApplicable(CouponMetricsSource source) {
        assertThat(source.stock().status()).isEqualTo(SourceStatus.N_A);
        assertThat(source.issuanceSamples().status()).isEqualTo(SourceStatus.N_A);
        assertThat(source.queue().status()).isEqualTo(SourceStatus.N_A);
        assertThat(source.holdingCounts().status()).isEqualTo(SourceStatus.N_A);
        assertThat(source.transitions().status()).isEqualTo(SourceStatus.N_A);
        assertThat(source.stock().value()).isNull();
        assertThat(source.issuanceSamples().value()).isNull();
    }

    private static final class FixedOverviewFactory extends AdminOverviewMockDataFactory {

        private final AdminOverviewMockDataset dataset;

        private FixedOverviewFactory(AdminOverviewMockDataset dataset) {
            this.dataset = dataset;
        }

        @Override
        public AdminOverviewMockDataset create(Instant snapshotAt) {
            return dataset;
        }
    }
}

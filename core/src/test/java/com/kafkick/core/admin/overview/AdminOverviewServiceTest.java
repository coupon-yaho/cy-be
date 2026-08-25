package com.kafkick.core.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.PreparationObservation;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource.StockCounts;
import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator.ActionCalculation;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.observation.OverviewObservationData;
import com.kafkick.core.admin.overview.observation.OverviewObservationRequest;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigCommand;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

class AdminOverviewServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-24T00:00:00Z");
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.5, Duration.ofMinutes(2), Duration.ofMinutes(10),
            Duration.ofMinutes(2), Duration.ofMinutes(10));

    @Test
    void readsCatalogAndCurrentRuntimeOnceAndPassesDatabaseIdsUnchangedToObservation() {
        RecordingReader reader = new RecordingReader(validCatalog(701L, 909L));
        RecordingRuntimeStore runtimeStore = new RecordingRuntimeStore();
        RecordingObservationSource observationSource = new RecordingObservationSource();

        AdminOverviewResult result = service(reader, runtimeStore, observationSource).getOverview();

        assertThat(reader.catalogCalls).isEqualTo(1);
        assertThat(reader.snapshotAt).isEqualTo(NOW);
        assertThat(runtimeStore.getCalls).isEqualTo(1);
        assertThat(runtimeStore.lastKnownGoodCalls).isZero();
        assertThat(observationSource.calls).isEqualTo(1);
        assertThat(observationSource.request.campaignTargets())
                .extracting(target -> target.couponId())
                .containsExactly(701L, 909L);
        assertThat(result.snapshot().campaigns().value())
                .extracting(AdminOverviewSnapshot.CampaignOverview::couponId)
                .containsExactly(701L, 909L);
    }

    @Test
    void makesCampaignSectionsUnavailableWhenCatalogIsUnavailable() {
        AdminCampaignCatalog unavailable = new AdminCampaignCatalog(
                SourceStatus.UNAVAILABLE, null, List.of());

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(unavailable), new RecordingRuntimeStore(),
                new RecordingObservationSource()).getOverview().snapshot();

        assertThat(snapshot.campaigns().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.campaignStatusSummary().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.openingSoon().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.stockRisk().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    void keepsOpeningSoonPendingWhenPreparationIsPending() {
        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(validCatalog(701L)), new RecordingRuntimeStore(),
                new RecordingObservationSource()).getOverview().snapshot();

        assertThat(snapshot.openingSoon().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.openingSoon().value()).isNull();
    }

    @Test
    void preservesOpenStockValueStatusAndObservedAtInTheObservationRequest() {
        AdminCampaignCatalog catalog = new AdminCampaignCatalog(SourceStatus.VALID, NOW, List.of(
                campaign(701L, CouponRoundStatus.OPEN,
                        new CouponMetricsSource.Observation<>(new StockCounts(10, 4), SourceStatus.VALID, NOW)),
                campaign(702L, CouponRoundStatus.OPEN,
                        new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null))));
        RecordingObservationSource source = new RecordingObservationSource();

        service(new RecordingReader(catalog), new RecordingRuntimeStore(), source).getOverview();

        assertThat(source.request.campaignTargets()).satisfiesExactly(
                target -> {
                    assertThat(target.couponId()).isEqualTo(701L);
                    assertThat(target.stockAvailable()).isTrue();
                    assertThat(target.stockStatus()).isEqualTo(SourceStatus.VALID);
                    assertThat(target.stockObservedAt()).isEqualTo(NOW);
                },
                target -> {
                    assertThat(target.couponId()).isEqualTo(702L);
                    assertThat(target.stockAvailable()).isNull();
                    assertThat(target.stockStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
                    assertThat(target.stockObservedAt()).isNull();
                });
    }

    @Test
    void makesOpenStockUnavailableWhenCurrentRuntimeIsUnavailableWithoutReadingLastKnownGood() {
        RecordingRuntimeStore runtimeStore = new RecordingRuntimeStore(new RuntimeConfigSnapshot(
                EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF, 1L,
                NOW, "test", SourceStatus.UNAVAILABLE));
        AdminCampaignCatalog catalog = new AdminCampaignCatalog(SourceStatus.VALID, NOW, List.of(
                campaign(701L, CouponRoundStatus.OPEN,
                        new CouponMetricsSource.Observation<>(new StockCounts(10, 4), SourceStatus.VALID, NOW))));

        AdminOverviewSnapshot.CampaignOverview row = service(
                new RecordingReader(catalog), runtimeStore,
                new RecordingObservationSource()).getOverview().snapshot().campaigns().value().getFirst();

        assertThat(row.stockForecast().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(runtimeStore.getCalls).isEqualTo(1);
        assertThat(runtimeStore.lastKnownGoodCalls).isZero();
    }

    @Test
    void rejectsObservationDataForAnotherSnapshotRequest() {
        OverviewObservationSource mismatched = requested -> {
            OverviewObservationRequest other = new OverviewObservationRequest(
                    requested.snapshotAt().minusSeconds(1), requested.campaignTargets(), requested.policy());
            return emptyObservationData(other);
        };

        assertThatThrownBy(() -> service(
                new RecordingReader(validCatalog(701L)), new RecordingRuntimeStore(), mismatched).getOverview())
                .isInstanceOfSatisfying(BusinessException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(AdminOverviewErrorCode.OBSERVATION_REQUEST_MISMATCH));
    }

    @Test
    void keepsActionSurfacesPendingButReusesStoppedIssuanceActionOnCampaignRow() {
        AdminCampaignCatalog catalog = new AdminCampaignCatalog(SourceStatus.VALID, NOW, List.of(
                campaign(701L, CouponRoundStatus.OPEN,
                        new CouponMetricsSource.Observation<>(new StockCounts(10, 4), SourceStatus.VALID, NOW))));
        OverviewObservationSource source = requested -> new OverviewObservationData(
                requested, List.of(stoppedFlow(701L)),
                new CustomerOutcomeCalculator.OutcomeInput(null, null, null, SourceStatus.PENDING, null),
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null),
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null));
        RecordingActionCalculator actionCalculator = new RecordingActionCalculator();
        AdminOverviewService service = new AdminOverviewService(
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)),
                new RecordingReader(catalog), new RecordingRuntimeStore(), POLICY, source,
                new IssuanceFlowCalculator(), new IssuanceActionCalculator(),
                new CampaignQueueCalculator(), new CustomerOutcomeCalculator(),
                new StockRiskCalculator(), new CampaignOverviewCalculator(), actionCalculator,
                new OverviewStatusCalculator());

        AdminOverviewResult result = service.getOverview();

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.PARTIAL);
        assertThat(result.snapshot().actionRequired().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.snapshot().actionItems().status()).isEqualTo(SourceStatus.PENDING);
        AdminOverviewSnapshot.RecommendedAction rowAction =
                result.snapshot().campaigns().value().getFirst().recommendedAction();
        assertThat(rowAction.code()).isEqualTo(AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED);
        assertThat(rowAction).isSameAs(actionCalculator.result.representativeByCoupon()
                .get(701L).recommendedAction());
    }

    @Test
    void reportsUnavailableWhenNoCoreSourceCarriesAValue() {
        AdminCampaignCatalog unavailable = new AdminCampaignCatalog(
                SourceStatus.UNAVAILABLE, null, List.of());

        AdminOverviewResult result = service(
                new RecordingReader(unavailable), new RecordingRuntimeStore(),
                new RecordingObservationSource()).getOverview();

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.UNAVAILABLE);
    }

    @Test
    void invokesEveryCalculatorExactlyOncePerOverviewRequest() {
        IssuanceFlowCalculator issuance = org.mockito.Mockito.spy(new IssuanceFlowCalculator());
        IssuanceActionCalculator issuanceAction = org.mockito.Mockito.spy(new IssuanceActionCalculator());
        CampaignQueueCalculator queue = org.mockito.Mockito.spy(new CampaignQueueCalculator());
        CustomerOutcomeCalculator outcome = org.mockito.Mockito.spy(new CustomerOutcomeCalculator());
        StockRiskCalculator stock = org.mockito.Mockito.spy(new StockRiskCalculator());
        CampaignOverviewCalculator campaign = org.mockito.Mockito.spy(new CampaignOverviewCalculator());
        OperationActionCalculator action = org.mockito.Mockito.spy(new OperationActionCalculator());
        OverviewStatusCalculator status = org.mockito.Mockito.spy(new OverviewStatusCalculator());
        AdminOverviewService service = new AdminOverviewService(
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)),
                new RecordingReader(validCatalog(701L)), new RecordingRuntimeStore(), POLICY,
                new RecordingObservationSource(), issuance, issuanceAction, queue, outcome, stock,
                campaign, action, status);

        service.getOverview();

        assertThat(List.of(issuance, issuanceAction, queue, outcome, stock, campaign, action, status))
                .allSatisfy(calculator -> assertThat(
                        org.mockito.Mockito.mockingDetails(calculator).getInvocations()).hasSize(1));
    }

    private static AdminOverviewService service(
            AdminCampaignDataReader reader,
            RuntimeConfigStore runtimeStore,
            OverviewObservationSource observationSource
    ) {
        return new AdminOverviewService(
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)), reader, runtimeStore, POLICY,
                observationSource, new IssuanceFlowCalculator(), new IssuanceActionCalculator(),
                new CampaignQueueCalculator(), new CustomerOutcomeCalculator(), new StockRiskCalculator(),
                new CampaignOverviewCalculator(), new OperationActionCalculator(),
                new OverviewStatusCalculator());
    }

    private static AdminCampaignCatalog validCatalog(Long... couponIds) {
        return new AdminCampaignCatalog(SourceStatus.VALID, NOW,
                java.util.Arrays.stream(couponIds)
                        .map(couponId -> new AdminCampaignCatalog.CampaignData(
                                couponId, "campaign-" + couponId, "brand",
                                CouponRoundStatus.SCHEDULED, NOW.plusSeconds(600), NOW.plusSeconds(3600),
                                new CouponMetricsSource.Observation<>(null, SourceStatus.N_A, null),
                                new PreparationObservation(null, SourceStatus.PENDING, null)))
                        .toList());
    }

    private static AdminCampaignCatalog.CampaignData campaign(
            long couponId,
            CouponRoundStatus status,
            CouponMetricsSource.Observation<StockCounts> stock
    ) {
        return new AdminCampaignCatalog.CampaignData(
                couponId, "campaign-" + couponId, "brand", status,
                NOW.minusSeconds(600), NOW.plusSeconds(3600), stock,
                new PreparationObservation(null, SourceStatus.PENDING, null));
    }

    private static IssuanceFlowInput stoppedFlow(long couponId) {
        return new IssuanceFlowInput(
                couponId, CouponRoundStatus.OPEN, true,
                NOW.minusSeconds(60), NOW,
                NOW.minusSeconds(600), NOW,
                5d, 0d, 5d,
                NOW.minusSeconds(120), NOW.minusSeconds(60),
                List.of(new IssuanceFlowCalculator.IssuanceBucket(
                        NOW.minusSeconds(60), NOW, 0d)),
                NOW.minusSeconds(180), NOW.minusSeconds(180),
                SourceStatus.VALID, NOW);
    }

    private static OverviewObservationData emptyObservationData(OverviewObservationRequest requested) {
        List<IssuanceFlowInput> flows = requested.campaignTargets().stream()
                .map(target -> {
                    SourceStatus flowStatus = target.campaignStatus() == CouponRoundStatus.OPEN
                            ? SourceStatus.UNAVAILABLE : SourceStatus.N_A;
                    return new IssuanceFlowInput(
                            target.couponId(), target.campaignStatus(), null,
                            null, null, null, null, null, null, null,
                            null, null, null, null, null, flowStatus, null);
                })
                .toList();
        return new OverviewObservationData(
                requested, flows,
                new CustomerOutcomeCalculator.OutcomeInput(
                        null, null, null, SourceStatus.PENDING, null),
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null),
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null));
    }

    private static final class RecordingReader implements AdminCampaignDataReader {

        private final AdminCampaignCatalog catalog;
        private int catalogCalls;
        private Instant snapshotAt;

        private RecordingReader(AdminCampaignCatalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public AdminCampaignCatalog loadCatalog(Instant requestedAt) {
            catalogCalls++;
            snapshotAt = requestedAt;
            return catalog;
        }

        @Override
        public AdminCampaignDetailData findDetail(long couponId, Instant from, Instant to, Instant requestedAt) {
            throw new AssertionError("Overview 요청에서 detail을 읽으면 안 됩니다.");
        }
    }

    private static final class RecordingRuntimeStore implements RuntimeConfigStore {

        private final RuntimeConfigSnapshot snapshot;
        private int getCalls;
        private int lastKnownGoodCalls;

        private RecordingRuntimeStore() {
            this(new RuntimeConfigSnapshot(EngineVersion.V1, ReleaseStage.V1, QueueMode.OFF,
                    1L, NOW, "test", SourceStatus.VALID));
        }

        private RecordingRuntimeStore(RuntimeConfigSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public RuntimeConfigSnapshot get() {
            getCalls++;
            return snapshot;
        }

        @Override
        public RuntimeConfigSnapshot update(RuntimeConfigCommand command, long expectedRevision) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<RuntimeConfigSnapshot> getLastKnownGood() {
            lastKnownGoodCalls++;
            throw new AssertionError("Overview는 last-known-good를 다시 읽으면 안 됩니다.");
        }
    }

    private static final class RecordingObservationSource implements OverviewObservationSource {

        private OverviewObservationRequest request;
        private int calls;

        @Override
        public OverviewObservationData observe(OverviewObservationRequest requested) {
            calls++;
            request = requested;
            return emptyObservationData(requested);
        }
    }

    private static final class RecordingActionCalculator extends OperationActionCalculator {

        private ActionCalculation result;

        @Override
        public ActionCalculation calculate(List<AdminOverviewSnapshot.OperationActionItem> decisions) {
            result = super.calculate(decisions);
            return result;
        }
    }
}

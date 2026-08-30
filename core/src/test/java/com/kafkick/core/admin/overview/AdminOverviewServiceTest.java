package com.kafkick.core.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.CouponPolicyType;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDetailData;
import com.kafkick.core.admin.couponroundsource.PreparationItem;
import com.kafkick.core.admin.couponroundsource.PreparationObservation;
import com.kafkick.core.admin.couponroundsource.PreparationSource;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource;
import com.kafkick.core.admin.couponmetrics.CouponMetricsSource.StockCounts;
import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.calculator.CouponRoundOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CouponRoundPreparationCalculator;
import com.kafkick.core.admin.overview.calculator.CouponRoundQueueCalculator;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionContext;
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
import com.kafkick.core.admin.queue.PendingAdminQueueObservationSource;
import com.kafkick.core.admin.preparation.AdminPreparationResolver;
import com.kafkick.core.admin.preparation.V2AdminPreparationReader;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.admin.stock.AdminStockResolver;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyFinalObservation;
import com.kafkick.core.consistency.ConsistencyFinalReader;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.Severity;
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
        assertThat(observationSource.request.couponRoundTargets())
                .extracting(target -> target.couponId())
                .containsExactly(701L, 909L);
        assertThat(result.snapshot().couponRounds().value())
                .extracting(AdminOverviewSnapshot.CouponRoundOverview::couponId)
                .containsExactly(701L, 909L);
    }

    @Test
    void makesCouponRoundSectionsUnavailableWhenCatalogIsUnavailable() {
        AdminCouponRoundCatalog unavailable = new AdminCouponRoundCatalog(
                SourceStatus.UNAVAILABLE, null, List.of());

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(unavailable), new RecordingRuntimeStore(),
                new RecordingObservationSource()).getOverview().snapshot();

        assertThat(snapshot.couponRounds().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.couponRoundStatusSummary().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.openingSoon().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.stockRisk().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    void keepsOpeningSoonPendingWhenPreparationIsPending() {
        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(validCatalog(701L)), new RecordingRuntimeStore(),
                new RecordingObservationSource())
                .getOverview().snapshot();

        assertThat(snapshot.openingSoon().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.openingSoon().value()).isNull();
        assertThat(snapshot.actionRequired().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.actionRequired().value()).isNull();
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.actionItems().value()).isNull();
    }

    /** 준비 원천 장애를 정상 빈 조치 목록으로 바꾸는 회귀를 방지합니다. */
    @Test
    void keepsActionSurfacesUnavailableWhenPreparationIsUnavailable() {
        AdminCouponRoundCatalog catalog = scheduledCatalog(
                new PreparationObservation(null, SourceStatus.UNAVAILABLE, null));

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(catalog), new RecordingRuntimeStore(),
                new RecordingObservationSource())
                .getOverview().snapshot();

        assertThat(snapshot.openingSoon().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.actionRequired().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.actionRequired().value()).isNull();
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.actionItems().value()).isNull();
    }

    /** 마지막 준비 미완료 값의 참고 조치는 유지하되 전체 조치 상태를 최신값으로 올리지 않습니다. */
    @Test
    void keepsStalePreparationActionAndActionSurfaceStateStale() {
        AdminCouponRoundCatalog catalog = scheduledCatalog(
                new PreparationObservation(false, SourceStatus.STALE, NOW.minusSeconds(1)));

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(catalog), new RecordingRuntimeStore(),
                new RecordingObservationSource())
                .getOverview().snapshot();

        assertThat(snapshot.openingSoon().observedAt()).isEqualTo(NOW.minusSeconds(1));
        assertThat(snapshot.actionRequired().status()).isEqualTo(SourceStatus.STALE);
        assertThat(snapshot.actionRequired().observedAt()).isEqualTo(NOW.minusSeconds(1));
        assertThat(snapshot.actionRequired().value().warningCount()).isEqualTo(1L);
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.STALE);
        assertThat(snapshot.actionItems().value().topItems().getFirst().recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.COUPON_ROUND_NOT_READY);
    }

    /** 준비 미완료 판정 하나가 네 운영 화면에서 서로 다른 결과로 갈라지는 회귀를 방지합니다. */
    @Test
    void linksValidIncompletePreparationToKpiActionListAndCouponRoundRow() {
        AdminCouponRoundCatalog catalog = scheduledCatalog(
                new PreparationObservation(false, SourceStatus.VALID, NOW));
        AdminOverviewService service = service(
                new RecordingReader(catalog), new RecordingRuntimeStore(),
                new RecordingObservationSource());

        AdminOverviewSnapshot snapshot = service.getOverview().snapshot();

        assertThat(snapshot.openingSoon().value().preparationIncompleteCount()).isEqualTo(1L);
        assertThat(snapshot.actionRequired().value().warningCount()).isEqualTo(1L);
        AdminOverviewSnapshot.OperationActionItem action =
                snapshot.actionItems().value().topItems().getFirst();
        assertThat(action.recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.COUPON_ROUND_NOT_READY);
        AdminOverviewSnapshot.CouponRoundOverview couponRound = snapshot.couponRounds().value().getFirst();
        assertThat(couponRound.severity()).isEqualTo(com.kafkick.core.observation.Severity.WARN);
        assertThat(couponRound.customerImpact()).isEqualTo(action.customerImpact());
        assertThat(couponRound.recommendedAction()).isSameAs(action.recommendedAction());
    }

    /** V2 게이트 실패가 기존 네 운영현황 영역에 같은 원인으로 연결되는지 검증합니다. */
    @Test
    void linksV2GateFailureToKpiActionAndCouponRoundRow() {
        RecordingReader catalogReader = new RecordingReader(v2ReadyCatalog(701L));
        V2AdminPreparationReader preparationReader = (requests, observedAt) -> {
            assertThat(requests).singleElement().satisfies(request -> {
                assertThat(request.couponId()).isEqualTo(701L);
                assertThat(request.expectedTotalQuantity()).isEqualTo(100L);
                assertThat(request.expectedRemainingQuantity()).isEqualTo(100L);
                assertThat(request.expectedGradeMask()).isEqualTo(3);
            });
            return Map.of(701L, new V2PreparationSource(
                    true, false, SourceStatus.VALID, NOW.minusSeconds(1L)));
        };

        AdminOverviewSnapshot snapshot = serviceWithPreparation(
                catalogReader, preparationReader).getOverview().snapshot();

        assertThat(catalogReader.catalogCalls).isEqualTo(1);
        assertThat(snapshot.openingSoon().value().preparationIncompleteCount()).isEqualTo(1L);
        assertThat(snapshot.actionRequired().value().warningCount()).isEqualTo(1L);
        AdminOverviewSnapshot.OperationActionItem action =
                snapshot.actionItems().value().topItems().getFirst();
        assertThat(action.recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.COUPON_ROUND_NOT_READY);
        assertThat(snapshot.couponRounds().value().getFirst().failedPreparationItems())
                .containsExactly(PreparationItem.REDIS_GATE);
    }

    /** 아직 워밍업 전인 V2 회차를 확정 실패 조치로 만드는 회귀를 방지합니다. */
    @Test
    void keepsV2PendingWithoutFalsePreparationAction() {
        V2AdminPreparationReader preparationReader = (requests, observedAt) -> Map.of(
                701L, new V2PreparationSource(null, null, SourceStatus.PENDING, null));

        AdminOverviewSnapshot snapshot = serviceWithPreparation(
                new RecordingReader(v2ReadyCatalog(701L)), preparationReader)
                .getOverview().snapshot();

        assertThat(snapshot.openingSoon().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(snapshot.couponRounds().value().getFirst().failedPreparationItems()).isEmpty();
    }

    /** V2 Redis 장애가 같은 목록의 V1 준비 완료를 실패 항목으로 바꾸지 않는지 검증합니다. */
    @Test
    void isolatesV2UnavailableFromV1Preparation() {
        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, List.of(
                readyCouponRound(701L, EngineVersion.V1), readyCouponRound(702L, EngineVersion.V2)));
        V2AdminPreparationReader preparationReader = (requests, observedAt) -> Map.of(
                702L, V2PreparationSource.unavailable());

        AdminOverviewSnapshot snapshot = serviceWithPreparation(
                new RecordingReader(catalog), preparationReader).getOverview().snapshot();

        assertThat(snapshot.openingSoon().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(snapshot.couponRounds().value())
                .allSatisfy(couponRound -> assertThat(couponRound.failedPreparationItems()).isEmpty());
    }

    @Test
    void preservesOpenStockValueStatusAndObservedAtInTheObservationRequest() {
        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, List.of(
                couponRound(701L, CouponRoundStatus.OPEN,
                        new CouponMetricsSource.Observation<>(new StockCounts(10, 4), SourceStatus.VALID, NOW)),
                couponRound(702L, CouponRoundStatus.OPEN,
                        new CouponMetricsSource.Observation<>(null, SourceStatus.UNAVAILABLE, null))));
        RecordingObservationSource source = new RecordingObservationSource();

        service(new RecordingReader(catalog), new RecordingRuntimeStore(), source).getOverview();

        assertThat(source.request.couponRoundTargets()).satisfiesExactly(
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
        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, List.of(
                couponRound(701L, CouponRoundStatus.OPEN,
                        new CouponMetricsSource.Observation<>(new StockCounts(10, 4), SourceStatus.VALID, NOW))));

        AdminOverviewSnapshot.CouponRoundOverview row = service(
                new RecordingReader(catalog), runtimeStore,
                new RecordingObservationSource()).getOverview().snapshot().couponRounds().value().getFirst();

        assertThat(row.stockForecast().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(runtimeStore.getCalls).isEqualTo(1);
        assertThat(runtimeStore.lastKnownGoodCalls).isZero();
    }

    @Test
    void rejectsObservationDataForAnotherSnapshotRequest() {
        OverviewObservationSource mismatched = requested -> {
            OverviewObservationRequest other = new OverviewObservationRequest(
                    requested.snapshotAt().minusSeconds(1), requested.couponRoundTargets(), requested.policy());
            return emptyObservationData(other);
        };

        assertThatThrownBy(() -> service(
                new RecordingReader(validCatalog(701L)), new RecordingRuntimeStore(), mismatched).getOverview())
                .isInstanceOfSatisfying(BusinessException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(AdminOverviewErrorCode.OBSERVATION_REQUEST_MISMATCH));
    }

    @Test
    void keepsActionSurfacesPendingButReusesStoppedIssuanceActionOnCouponRoundRow() {
        AdminCouponRoundCatalog catalog = new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, List.of(
                couponRound(701L, CouponRoundStatus.OPEN,
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
                new PendingAdminQueueObservationSource(),
                new IssuanceFlowCalculator(), new IssuanceActionCalculator(),
                new CouponRoundQueueCalculator(), new CustomerOutcomeCalculator(),
                new StockRiskCalculator(), new CouponRoundOverviewCalculator(),
                notApplicableFinalReader(), new ConsistencyActionCalculator(), actionCalculator,
                new OverviewStatusCalculator());

        AdminOverviewResult result = service.getOverview();

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.PARTIAL);
        assertThat(result.snapshot().actionRequired().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.snapshot().actionItems().status()).isEqualTo(SourceStatus.PENDING);
        AdminOverviewSnapshot.RecommendedAction rowAction =
                result.snapshot().couponRounds().value().getFirst().recommendedAction();
        assertThat(rowAction.code()).isEqualTo(AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED);
        assertThat(rowAction).isSameAs(actionCalculator.result.representativeByCoupon()
                .get(701L).recommendedAction());
    }

    @Test
    void reportsUnavailableWhenNoCoreSourceCarriesAValue() {
        AdminCouponRoundCatalog unavailable = new AdminCouponRoundCatalog(
                SourceStatus.UNAVAILABLE, null, List.of());

        AdminOverviewResult result = service(
                new RecordingReader(unavailable), new RecordingRuntimeStore(),
                new RecordingObservationSource()).getOverview();

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.UNAVAILABLE);
    }

    @Test
    void invokesEveryCalculationBoundaryExactlyOncePerOverviewRequest() {
        IssuanceFlowCalculator issuance = org.mockito.Mockito.spy(new IssuanceFlowCalculator());
        IssuanceActionCalculator issuanceAction = org.mockito.Mockito.spy(new IssuanceActionCalculator());
        CouponRoundQueueCalculator queue = org.mockito.Mockito.spy(new CouponRoundQueueCalculator());
        CustomerOutcomeCalculator outcome = org.mockito.Mockito.spy(new CustomerOutcomeCalculator());
        StockRiskCalculator stock = org.mockito.Mockito.spy(new StockRiskCalculator());
        CouponRoundOverviewCalculator couponRound = org.mockito.Mockito.spy(new CouponRoundOverviewCalculator());
        OperationActionCalculator action = org.mockito.Mockito.spy(new OperationActionCalculator());
        ConsistencyActionCalculator consistencyAction =
                org.mockito.Mockito.spy(new ConsistencyActionCalculator());
        OverviewStatusCalculator status = org.mockito.Mockito.spy(new OverviewStatusCalculator());
        AdminOverviewService service = new AdminOverviewService(
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)),
                new RecordingReader(validCatalog(701L)), new RecordingRuntimeStore(), POLICY,
                new RecordingObservationSource(), new PendingAdminQueueObservationSource(),
                issuance, issuanceAction, queue, outcome, stock,
                couponRound, notApplicableFinalReader(), consistencyAction, action, status);

        service.getOverview();

        assertThat(List.of(issuance, issuanceAction, queue, outcome, stock, consistencyAction, action, status))
                .allSatisfy(calculator -> assertThat(
                        org.mockito.Mockito.mockingDetails(calculator).getInvocations()).hasSize(1));
        assertThat(org.mockito.Mockito.mockingDetails(couponRound).getInvocations())
                .extracting(invocation -> invocation.getMethod().getName())
                .containsExactly("calculatePreparation", "calculate");
    }

    @Test
    void readsLatestFinalsOnceForTheDatabaseCouponRoundPopulation() {
        RecordingFinalReader finalReader = new RecordingFinalReader(Map.of(
                701L, finalObservation(SourceStatus.N_A, null),
                909L, finalObservation(SourceStatus.N_A, null)));

        service(new RecordingReader(validCatalog(701L, 909L)), new RecordingRuntimeStore(),
                new RecordingObservationSource(), finalReader).getOverview();

        assertThat(finalReader.calls).isEqualTo(1);
        assertThat(finalReader.requestedIds).containsExactly(701L, 909L);
    }

    @Test
    void rejectsFinalResponseForAnotherCouponRoundPopulation() {
        RecordingFinalReader finalReader = new RecordingFinalReader(Map.of(
                701L, finalObservation(SourceStatus.N_A, null)));

        assertThatThrownBy(() -> service(
                new RecordingReader(validCatalog(701L, 909L)), new RecordingRuntimeStore(),
                new RecordingObservationSource(), finalReader).getOverview())
                .isInstanceOfSatisfying(BusinessException.class,
                        failure -> assertThat(failure.getErrorCode())
                                .isEqualTo(AdminOverviewErrorCode.OBSERVATION_REQUEST_MISMATCH));
    }

    @Test
    void linksFinalFailureToKpiListAndCouponRoundRowWithEvaluatedAt() {
        Instant evaluatedAt = NOW.minusSeconds(30);
        RecordingFinalReader finalReader = new RecordingFinalReader(Map.of(
                701L, validFailedFinal(701L, 0L, 1L, evaluatedAt)));

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(readyCatalog(701L)), new RecordingRuntimeStore(),
                new RecordingObservationSource(), finalReader).getOverview().snapshot();

        assertThat(snapshot.actionRequired().value().totalCount()).isEqualTo(1L);
        assertThat(snapshot.actionRequired().value().urgentCount()).isEqualTo(1L);
        AdminOverviewSnapshot.OperationActionItem action =
                snapshot.actionItems().value().topItems().getFirst();
        assertThat(action.detectedAt()).isEqualTo(evaluatedAt);
        assertThat(snapshot.actionRequired().observedAt()).isEqualTo(evaluatedAt);
        assertThat(snapshot.actionItems().observedAt()).isEqualTo(evaluatedAt);
        assertThat(action.couponName()).isEqualTo("couponRound-701");
        AdminOverviewSnapshot.CouponRoundOverview row = snapshot.couponRounds().value().getFirst();
        assertThat(row.recommendedAction()).isSameAs(action.recommendedAction());
    }

    @Test
    void excludesEveryFinalCandidateButKeepsOtherActionsWhenOneFinalIsPending() {
        RecordingFinalReader finalReader = new RecordingFinalReader(Map.of(
                701L, validFailedFinal(701L, 3L, 0L, NOW.minusSeconds(30)),
                909L, finalObservation(SourceStatus.PENDING, null)));

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(readyCatalog(701L, 909L)), new RecordingRuntimeStore(),
                new RecordingObservationSource(), finalReader).getOverview().snapshot();

        assertThat(snapshot.actionRequired().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.actionRequired().value().totalCount()).isZero();
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.actionItems().value().topItems()).isEmpty();
        assertThat(snapshot.couponRounds().value())
                .allSatisfy(row -> assertThat(row.recommendedAction()).isNull());
    }

    @Test
    void excludesEveryFinalCandidateButKeepsOtherActionsWhenOneFinalIsUnavailable() {
        RecordingFinalReader finalReader = new RecordingFinalReader(Map.of(
                701L, validFailedFinal(701L, 0L, 1L, NOW.minusSeconds(30)),
                909L, finalObservation(SourceStatus.UNAVAILABLE, null)));

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(readyCatalog(701L, 909L)), new RecordingRuntimeStore(),
                new RecordingObservationSource(), finalReader).getOverview().snapshot();

        assertThat(snapshot.actionRequired().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.actionRequired().value().totalCount()).isZero();
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.actionItems().value().topItems()).isEmpty();
        assertThat(snapshot.couponRounds().value())
                .allSatisfy(row -> assertThat(row.recommendedAction()).isNull());
    }

    @Test
    void linksOverIssuedFinalAsWidespreadAction() {
        RecordingFinalReader finalReader = new RecordingFinalReader(Map.of(
                701L, validFailedFinal(701L, 3L, 0L, NOW.minusSeconds(30))));

        AdminOverviewSnapshot snapshot = service(
                new RecordingReader(readyCatalog(701L)), new RecordingRuntimeStore(),
                new RecordingObservationSource(), finalReader).getOverview().snapshot();

        AdminOverviewSnapshot.OperationActionItem action =
                snapshot.actionItems().value().topItems().getFirst();
        assertThat(action.customerImpact())
                .isEqualTo(AdminOverviewSnapshot.CustomerImpact.WIDESPREAD);
        assertThat(snapshot.couponRounds().value().getFirst().recommendedAction())
                .isSameAs(action.recommendedAction());
    }

    private static AdminOverviewService service(
            AdminCouponRoundDataReader reader,
            RuntimeConfigStore runtimeStore,
            OverviewObservationSource observationSource
    ) {
        return service(reader, runtimeStore, observationSource, notApplicableFinalReader());
    }

    /** 실제 준비 Resolver와 기본 V2 재고 fallback을 연결한 V2 서비스 fixture를 생성합니다. */
    private static AdminOverviewService serviceWithPreparation(
            AdminCouponRoundDataReader reader,
            V2AdminPreparationReader preparationReader
    ) {
        return new AdminOverviewService(
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)), reader,
                new RecordingRuntimeStore(), POLICY, new RecordingObservationSource(),
                new PendingAdminQueueObservationSource(),
                new IssuanceFlowCalculator(), new IssuanceActionCalculator(),
                new CouponRoundQueueCalculator(), new CustomerOutcomeCalculator(), new StockRiskCalculator(),
                new CouponRoundOverviewCalculator(), new CouponRoundPreparationCalculator(),
                notApplicableFinalReader(), new ConsistencyActionCalculator(),
                new OperationActionCalculator(), new OverviewStatusCalculator(),
                new AdminStockResolver(AdminStockResolver.unavailableV2Reader()),
                new AdminPreparationResolver(preparationReader));
    }

    private static AdminOverviewService service(
            AdminCouponRoundDataReader reader,
            RuntimeConfigStore runtimeStore,
            OverviewObservationSource observationSource,
            ConsistencyFinalReader finalReader
    ) {
        return new AdminOverviewService(
                new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)), reader, runtimeStore, POLICY,
                observationSource, new PendingAdminQueueObservationSource(),
                new IssuanceFlowCalculator(), new IssuanceActionCalculator(),
                new CouponRoundQueueCalculator(), new CustomerOutcomeCalculator(), new StockRiskCalculator(),
                new CouponRoundOverviewCalculator(), finalReader, new ConsistencyActionCalculator(),
                new OperationActionCalculator(),
                new OverviewStatusCalculator());
    }

    private static ConsistencyFinalReader notApplicableFinalReader() {
        return couponIds -> couponIds.stream().collect(java.util.stream.Collectors.toMap(
                couponId -> couponId,
                couponId -> finalObservation(SourceStatus.N_A, null),
                (left, right) -> left,
                java.util.LinkedHashMap::new));
    }

    private static AdminCouponRoundCatalog validCatalog(Long... couponIds) {
        return new AdminCouponRoundCatalog(SourceStatus.VALID, NOW,
                java.util.Arrays.stream(couponIds)
                        .map(couponId -> new AdminCouponRoundCatalog.CouponRoundData(
                                couponId, "couponRound-" + couponId, "brand",
                                CouponRoundStatus.SCHEDULED, NOW.plusSeconds(600), NOW.plusSeconds(3600),
                                new CouponMetricsSource.Observation<>(null, SourceStatus.N_A, null),
                                new PreparationObservation(null, SourceStatus.PENDING, null)))
                        .toList());
    }

    private static AdminCouponRoundCatalog scheduledCatalog(PreparationObservation preparation) {
        return new AdminCouponRoundCatalog(SourceStatus.VALID, NOW, List.of(
                new AdminCouponRoundCatalog.CouponRoundData(
                        701L, "couponRound-701", "brand", CouponRoundStatus.SCHEDULED,
                        NOW.plusSeconds(600), NOW.plusSeconds(3600),
                        new CouponMetricsSource.Observation<>(null, SourceStatus.N_A, null),
                        preparation)));
    }

    private static AdminCouponRoundCatalog readyCatalog(Long... couponIds) {
        return new AdminCouponRoundCatalog(SourceStatus.VALID, NOW,
                java.util.Arrays.stream(couponIds)
                        .map(couponId -> new AdminCouponRoundCatalog.CouponRoundData(
                                couponId, "couponRound-" + couponId, "brand",
                                CouponRoundStatus.SCHEDULED, NOW.plusSeconds(600), NOW.plusSeconds(3600),
                                new CouponMetricsSource.Observation<>(null, SourceStatus.N_A, null),
                                new PreparationObservation(true, SourceStatus.VALID, NOW)))
                        .toList());
    }

    /** Redis 준비 비교값을 모두 가진 V2 예약 회차 카탈로그를 생성합니다. */
    private static AdminCouponRoundCatalog v2ReadyCatalog(long couponId) {
        return new AdminCouponRoundCatalog(
                SourceStatus.VALID, NOW, List.of(readyCouponRound(couponId, EngineVersion.V2)));
    }

    /** DB 설정·재고 준비가 완료된 오픈 임박 회차를 지정한 엔진으로 생성합니다. */
    private static AdminCouponRoundCatalog.CouponRoundData readyCouponRound(
            long couponId,
            EngineVersion engineVersion
    ) {
        return new AdminCouponRoundCatalog.CouponRoundData(
                couponId, "couponRound-" + couponId, "brand", engineVersion,
                CouponRoundStatus.SCHEDULED, NOW.plusSeconds(600L), NOW.plusSeconds(3_600L),
                new CouponMetricsSource.Observation<>(
                        new StockCounts(100L, 0L), SourceStatus.VALID, NOW),
                new PreparationSource(
                        true, true, CouponPolicyType.FIXED_AMOUNT, 3, SourceStatus.VALID, NOW));
    }

    private static ConsistencyFinalObservation validFailedFinal(
            long couponId,
            long overIssued,
            long gap,
            Instant evaluatedAt
    ) {
        Map<ConsistencyGapType, GapValue> gaps = new EnumMap<>(ConsistencyGapType.class);
        for (ConsistencyGapType gapType : ConsistencyGapType.values()) {
            gaps.put(gapType, new GapValue(gap, SourceStatus.VALID, evaluatedAt));
        }
        ConsistencyEvaluation evaluation = new ConsistencyEvaluation(
                gaps, new GapValue(overIssued, SourceStatus.VALID, evaluatedAt),
                ConsistencyPhase.FINAL, Verdict.FAIL, Severity.CRITICAL);
        return finalObservation(SourceStatus.VALID, new ConsistencyActionContext(
                couponId, "stored-name", NOW.minusSeconds(600), evaluatedAt,
                EngineVersion.V2, evaluation));
    }

    private static ConsistencyFinalObservation finalObservation(
            SourceStatus status,
            ConsistencyActionContext value
    ) {
        return new ConsistencyFinalObservation(status, value);
    }

    private static AdminCouponRoundCatalog.CouponRoundData couponRound(
            long couponId,
            CouponRoundStatus status,
            CouponMetricsSource.Observation<StockCounts> stock
    ) {
        return new AdminCouponRoundCatalog.CouponRoundData(
                couponId, "couponRound-" + couponId, "brand", status,
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
        List<IssuanceFlowInput> flows = requested.couponRoundTargets().stream()
                .map(target -> {
                    SourceStatus flowStatus = target.couponRoundStatus() == CouponRoundStatus.OPEN
                            ? SourceStatus.UNAVAILABLE : SourceStatus.N_A;
                    return new IssuanceFlowInput(
                            target.couponId(), target.couponRoundStatus(), null,
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

    private static final class RecordingReader implements AdminCouponRoundDataReader {

        private final AdminCouponRoundCatalog catalog;
        private int catalogCalls;
        private Instant snapshotAt;

        private RecordingReader(AdminCouponRoundCatalog catalog) {
            this.catalog = catalog;
        }

        @Override
        public AdminCouponRoundCatalog loadCatalog(Instant requestedAt) {
            catalogCalls++;
            snapshotAt = requestedAt;
            return catalog;
        }

        @Override
        public AdminCouponRoundDetailData findDetail(long couponId, Instant from, Instant to, Instant requestedAt) {
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

    private static final class RecordingFinalReader implements ConsistencyFinalReader {

        private final Map<Long, ConsistencyFinalObservation> result;
        private int calls;
        private List<Long> requestedIds;

        private RecordingFinalReader(Map<Long, ConsistencyFinalObservation> result) {
            this.result = result;
        }

        @Override
        public Map<Long, ConsistencyFinalObservation> findLatestByCouponIds(List<Long> couponIds) {
            calls++;
            requestedIds = List.copyOf(couponIds);
            return result;
        }
    }

}

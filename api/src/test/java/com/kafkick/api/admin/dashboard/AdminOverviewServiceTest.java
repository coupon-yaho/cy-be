package com.kafkick.api.admin.dashboard;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewResult;
import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator.CampaignCalculation;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueCalculation;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueInput;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionContext;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeCalculation;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeInput;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowCalculation;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator.ActionCalculation;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator.StockInput;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator.StockRiskCalculation;
import com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/** Mock 캠페인 계산값과 미연결 관측값을 함께 조립하는 관리자 운영현황 Service를 검증합니다. */
class AdminOverviewServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-20T03:15:00Z");

    /** O1~O4와 Action의 같은 계산 결과가 KPI·목록·행에 재사용되는지 검증합니다. */
    @Test
    @DisplayName("Mock O1 O2 O3 O4 결과와 전체 관측값을 COMPLETE 운영현황으로 조립한다")
    void assemblesMockCalculationResultsAsCompleteOverview() {
        AdminOverviewService service = service();

        AdminOverviewResult result = service.getOverview();
        AdminOverviewSnapshot snapshot = result.snapshot();

        assertThat(snapshot.snapshotAt()).isEqualTo(NOW);
        assertThat(result.overallStatus()).isEqualTo(OverallStatus.COMPLETE);
        assertThat(snapshot.campaignStatusSummary().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.campaignStatusSummary().value())
                .isEqualTo(new AdminOverviewSnapshot.CampaignStatusSummary(3, 2, 1));
        assertThat(snapshot.openingSoon().value())
                .isEqualTo(new AdminOverviewSnapshot.OpeningSoonSummary(2, 1));
        assertThat(snapshot.actionRequired().value())
                .isEqualTo(new AdminOverviewSnapshot.ActionRequiredSummary(4, 3, 1));
        assertThat(snapshot.actionItems().value().topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                .containsExactly(101L, 102L, 103L, 105L);
        assertThat(snapshot.actionItems().value().topItems())
                .filteredOn(action -> action.couponId().equals(102L))
                .singleElement()
                .satisfies(action -> {
                    assertThat(action.customerImpact()).isEqualTo(
                            AdminOverviewSnapshot.CustomerImpact.LIMITED);
                    assertThat(action.recommendedAction().code()).isEqualTo(
                            AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE);
                });
        assertThat(snapshot.actionItems().value().topItems())
                .filteredOn(action -> action.couponId().equals(103L))
                .singleElement()
                .satisfies(action -> {
                    assertThat(action.customerImpact()).isEqualTo(
                            AdminOverviewSnapshot.CustomerImpact.WIDESPREAD);
                    assertThat(action.recommendedAction().code()).isEqualTo(
                            AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE);
                });
        assertThat(snapshot.campaigns().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.campaigns().value()).hasSize(6);
        assertThat(snapshot.campaigns().value())
                .extracting(AdminOverviewSnapshot.CampaignOverview::couponId)
                .containsExactly(101L, 102L, 103L, 105L, 104L, 106L);
        assertThat(snapshot.campaigns().value().getFirst())
                .satisfies(campaign -> {
                    assertThat(campaign.priority()).isEqualTo(1);
                    assertThat(campaign.severity()).isEqualTo(Severity.CRITICAL);
                    assertThat(campaign.recommendedAction().code())
                            .isEqualTo(AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED);
                    assertThat(campaign.campaignQueueStatus().value().assessment())
                            .isEqualTo(AdminOverviewSnapshot.CampaignQueueAssessment.ADMISSION_STOPPED);
                });
        assertThat(snapshot.campaigns().value())
                .filteredOn(campaign -> campaign.couponId().equals(102L))
                .singleElement()
                .satisfies(campaign -> {
                    assertThat(campaign.issuanceFlow().value().currentPerMinute()).isEqualTo(44.0);
                    assertThat(campaign.stockForecast().value())
                        .isEqualTo(new AdminOverviewSnapshot.StockForecast(
                                350L, 7_000L, 0.05, Duration.ofSeconds(478)));
                    assertThat(campaign.severity()).isEqualTo(Severity.CRITICAL);
                    assertThat(campaign.customerImpact()).isEqualTo(
                            AdminOverviewSnapshot.CustomerImpact.LIMITED);
                    assertThat(campaign.recommendedAction().code()).isEqualTo(
                            AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE);
                });
        assertThat(snapshot.campaigns().value())
                .filteredOn(campaign -> campaign.couponId().equals(103L))
                .singleElement()
                .satisfies(campaign -> {
                    assertThat(campaign.campaignQueueStatus().value().estimatedWait())
                            .isEqualTo(Duration.ofSeconds(40));
                    assertThat(campaign.stockForecast().status()).isEqualTo(SourceStatus.VALID);
                    assertThat(campaign.severity()).isEqualTo(Severity.CRITICAL);
                    assertThat(campaign.customerImpact()).isEqualTo(
                            AdminOverviewSnapshot.CustomerImpact.WIDESPREAD);
                    assertThat(campaign.recommendedAction().code()).isEqualTo(
                            AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE);
                });
        assertThat(snapshot.campaigns().value())
                .filteredOn(campaign -> campaign.couponId().equals(104L))
                .singleElement()
                .satisfies(campaign -> {
                    assertThat(campaign.issuanceFlow().status()).isEqualTo(SourceStatus.N_A);
                    assertThat(campaign.campaignQueueStatus().status()).isEqualTo(SourceStatus.N_A);
                    assertThat(campaign.stockForecast().status()).isEqualTo(SourceStatus.N_A);
                });
        assertThat(snapshot.queueRisk().value())
                .isEqualTo(new AdminOverviewSnapshot.QueueRiskSummary(1, null));
        assertThat(snapshot.stockRisk().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.aggregateQueue().value())
                .isEqualTo(new AdminOverviewSnapshot.AggregateQueue(3_388L, 4.6, null));
        assertThat(snapshot.customerOutcomes().status()).isEqualTo(SourceStatus.VALID);
        assertThat(snapshot.customerOutcomes().value().outcomes()).hasSize(7);
        assertThat(snapshot.aggregateIssuanceRate())
                .satisfies(observation -> {
                    assertThat(observation.value().currentPerSecond()).isGreaterThan(0.0);
                    assertThat(observation.status()).isEqualTo(SourceStatus.VALID);
                    assertThat(observation.observedAt()).isEqualTo(NOW);
                });
        assertThat(snapshot.latencySummary())
                .satisfies(observation -> {
                    assertThat(observation.value().successfulP99()).isPositive();
                    assertThat(observation.value().failedP99()).isPositive();
                    assertThat(observation.value().windowEnd()).isEqualTo(NOW);
                    assertThat(observation.status()).isEqualTo(SourceStatus.VALID);
                    assertThat(observation.observedAt()).isEqualTo(NOW);
                });
    }

    /**
     * 한 요청에서 각 협력자를 한 번씩만 호출하고 Calculator가 만든 Observation 객체를 새로 감싸지
     * 않고 Snapshot과 Campaign 행에 그대로 재사용하는지 검증합니다.
     */
    @Test
    @DisplayName("Service는 Calculator를 한 번씩 호출하고 O1 O2 O3 O4 Observation을 그대로 조립한다")
    void invokesCollaboratorsOnceAndPreservesCalculatedObservationIdentity() {
        List<String> events = new ArrayList<>();
        RecordingTimeProvider timeProvider = new RecordingTimeProvider(events);
        RecordingMockDataFactory factory = new RecordingMockDataFactory(events);
        RecordingIssuanceFlowCalculator issuance = new RecordingIssuanceFlowCalculator(events);
        RecordingIssuanceActionCalculator issuanceAction = new RecordingIssuanceActionCalculator(events);
        RecordingQueueCalculator queue = new RecordingQueueCalculator(events);
        RecordingOutcomeCalculator outcome = new RecordingOutcomeCalculator(events);
        RecordingStockRiskCalculator stock = new RecordingStockRiskCalculator(events);
        RecordingCampaignOverviewCalculator campaign = new RecordingCampaignOverviewCalculator(events);
        RecordingConsistencyActionCalculator consistency = new RecordingConsistencyActionCalculator(events);
        RecordingActionCalculator action = new RecordingActionCalculator(events);
        RecordingOverviewStatusCalculator status = new RecordingOverviewStatusCalculator(events);
        AdminOverviewService service = new AdminOverviewService(timeProvider, factory, issuance, issuanceAction, queue, outcome,
                stock, campaign, consistency, action, status);

        AdminOverviewSnapshot snapshot = service.getOverview().snapshot();

        assertThat(events).containsExactly(
                "time", "factory", "issuance", "issuanceAction", "queue", "outcome", "stock", "consistency",
                "consistency", "consistency", "action", "campaign", "status");
        assertThat(factory.createCount).isEqualTo(1);
        assertThat(issuance.calculateCount).isEqualTo(1);
        assertThat(issuanceAction.calculateCount).isEqualTo(1);
        assertThat(queue.calculateCount).isEqualTo(1);
        assertThat(outcome.calculateCount).isEqualTo(1);
        assertThat(stock.calculateCount).isEqualTo(1);
        assertThat(consistency.calculateCount).isEqualTo(3);
        assertThat(consistency.contexts).extracting(ConsistencyActionContext::couponId)
                .containsExactly(101L, 102L, 103L);
        assertThat(action.calculateCount).isEqualTo(1);
        assertThat(campaign.calculateCount).isEqualTo(1);
        assertThat(status.calculateCount).isEqualTo(1);
        assertThat(stock.inputs.stream()
                .filter(input -> input.couponId().equals(101L))
                .findFirst()
                .orElseThrow()
                .issuanceFlow())
                .isSameAs(issuance.result.issuanceFlows().get(101L));
        assertThat(snapshot.queueRisk()).isSameAs(queue.result.queueRisk());
        assertThat(snapshot.aggregateQueue()).isSameAs(queue.result.aggregateQueue());
        assertThat(snapshot.stockRisk()).isSameAs(stock.result.stockRisk());
        assertThat(snapshot.customerOutcomes()).isSameAs(outcome.result.customerOutcomes());
        assertThat(snapshot.campaigns().value().stream()
                .filter(row -> row.couponId().equals(101L))
                .findFirst()
                .orElseThrow()
                .campaignQueueStatus())
                .isSameAs(queue.result.queueStatuses().get(101L));
    }

    /** O2 모집단 상태가 Action KPI의 부분 합계 노출 여부와 최신성 상태를 결정하는지 검증합니다. */
    @Test
    @DisplayName("O2 상태는 Action KPI에 VALID STALE WARMING_UP 또는 값 없는 상태로 전파된다")
    void propagatesQueueCompletenessToActionObservations() {
        for (SourceStatus status : List.of(SourceStatus.STALE, SourceStatus.WARMING_UP,
                SourceStatus.UNAVAILABLE, SourceStatus.PENDING)) {
            AdminOverviewResult result = serviceWithQueueStatus(101L, status).getOverview();
            assertThat(result.snapshot().actionRequired().status()).isEqualTo(status);
            assertThat(result.snapshot().actionItems().status()).isEqualTo(status);
            if (status.carriesValue()) {
                assertThat(result.snapshot().actionRequired().value().totalCount()).isEqualTo(4L);
                assertThat(result.snapshot().actionItems().value().topItems())
                        .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                        .containsExactly(101L, 102L, 103L, 105L);
                assertThat(result.snapshot().actionRequired().observedAt()).isEqualTo(NOW);
                assertThat(result.snapshot().actionItems().observedAt()).isEqualTo(NOW);
            } else {
                assertThat(result.snapshot().actionRequired().value()).isNull();
                assertThat(result.snapshot().actionItems().value()).isNull();
                assertThat(result.snapshot().actionRequired().observedAt()).isNull();
                assertThat(result.snapshot().actionItems().observedAt()).isNull();
            }
            assertThat(campaignForCoupon(result, 101L).campaignQueueStatus().status()).isEqualTo(status);
            assertThat(campaignForCoupon(result, 101L).severity()).isEqualTo(Severity.CRITICAL);
            if (!status.carriesValue()) {
                assertThat(campaignForCoupon(result, 105L).severity()).isEqualTo(Severity.WARN);
                assertThat(campaignForCoupon(result, 105L).recommendedAction().code())
                        .isEqualTo(AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY);
            }
        }
        for (SourceStatus status : List.of(SourceStatus.N_A, SourceStatus.VALID, SourceStatus.NO_TRAFFIC)) {
            AdminOverviewResult result = serviceWithQueueStatus(102L, status).getOverview();
            assertThat(result.snapshot().actionRequired().status()).isEqualTo(SourceStatus.VALID);
            assertThat(result.snapshot().actionItems().status()).isEqualTo(SourceStatus.VALID);
            assertThat(result.snapshot().actionRequired().value().totalCount()).isEqualTo(4L);
            assertThat(result.snapshot().actionItems().value().topItems())
                    .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                    .containsExactly(101L, 102L, 103L, 105L);
            assertThat(result.snapshot().actionRequired().observedAt()).isEqualTo(NOW);
            assertThat(result.snapshot().actionItems().observedAt()).isEqualTo(NOW);
            assertThat(campaignForCoupon(result, 102L).campaignQueueStatus().status()).isEqualTo(status);
            assertThat(campaignForCoupon(result, 101L).severity()).isEqualTo(Severity.CRITICAL);
        }
    }

    /** O1 중단 후보가 기존 O2·준비 미완료 후보와 같은 대표 모집단을 쓰는지 검증합니다. */
    @Test
    @DisplayName("VALID STOPPED O1은 조치 KPI 목록과 캠페인 행에 같은 대표 조치로 연결된다")
    void combinesStoppedIssuanceActionWithExistingActionPopulation() {
        AdminOverviewResult result = serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset create(Instant snapshotAt) {
                com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> issuanceInputs = base.issuanceFlowInputs().stream()
                        .map(input -> input.couponId().equals(103L)
                                ? stoppedIssuanceInput(input, snapshotAt) : input)
                        .toList();
                return new com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset(base.policy(),
                        issuanceInputs, base.queueInputs(), base.outcomeInput(), base.campaigns(),
                        base.preparationActionCandidates(), base.consistencyActionContexts(), base.aggregateIssuanceRate(),
                        base.latencySummary());
            }
        }).getOverview();

        assertThat(campaignForCoupon(result, 103L).issuanceFlow().value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.STOPPED);
        AdminOverviewSnapshot.OperationActionItem issuanceAction = result.snapshot().actionItems().value().topItems()
                .stream()
                .filter(action -> action.couponId().equals(103L))
                .findFirst()
                .orElseThrow();
        assertThat(result.snapshot().actionRequired().value())
                .isEqualTo(new AdminOverviewSnapshot.ActionRequiredSummary(4, 3, 1));
        assertThat(issuanceAction.recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED);
        assertThat(issuanceAction.campaignName()).isEqualTo("정상 발급 감소 대기 쿠폰");
        assertThat(issuanceAction.opensAt()).isEqualTo(NOW.minus(Duration.ofMinutes(10)));
        assertThat(issuanceAction.detectedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(10)));
        assertThat(campaignForCoupon(result, 103L).recommendedAction()).isSameAs(issuanceAction.recommendedAction());
        assertThat(campaignForCoupon(result, 103L).severity()).isEqualTo(Severity.CRITICAL);
    }

    /** O1 원천의 값 없음·최신성 상태가 O2만으로 만든 정상 조치 KPI를 덮는지 검증합니다. */
    @Test
    @DisplayName("O1 원천 상태는 Action KPI 목록의 완전성과 가장 오래된 관측 시각에 반영된다")
    void propagatesIssuanceCompletenessToActionObservations() {
        for (SourceStatus status : List.of(SourceStatus.STALE, SourceStatus.WARMING_UP)) {
            AdminOverviewResult result = serviceWithIssuanceStatus(102L, status).getOverview();

            assertThat(result.snapshot().actionRequired().status()).isEqualTo(status);
            assertThat(result.snapshot().actionItems().status()).isEqualTo(status);
            assertThat(result.snapshot().actionRequired().value().totalCount()).isEqualTo(4L);
            assertThat(result.snapshot().actionItems().value().topItems())
                    .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                    .containsExactly(101L, 102L, 103L, 105L);
            assertThat(result.snapshot().actionRequired().observedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(5)));
            assertThat(result.snapshot().actionItems().observedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(5)));
        }
        for (SourceStatus status : List.of(SourceStatus.PENDING, SourceStatus.UNAVAILABLE)) {
            AdminOverviewResult result = serviceWithIssuanceStatus(102L, status).getOverview();

            assertThat(result.snapshot().actionRequired())
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, status, null));
            assertThat(result.snapshot().actionItems())
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, status, null));
        }
    }

    /** O1·O2의 혼합 원천 상태 우선순위와 적용 대상 없는 경계를 Action 관측 계약으로 고정합니다. */
    @Test
    @DisplayName("Action 상태는 O1 O2 혼합 우선순위와 전체 N_A NO_TRAFFIC 의미를 보존한다")
    void combinesAllActionSourceStatusesWithoutHidingPreparationActions() {
        assertActionSourceStatus(SourceStatus.UNAVAILABLE, SourceStatus.PENDING, SourceStatus.UNAVAILABLE, null);
        assertActionSourceStatus(SourceStatus.PENDING, SourceStatus.STALE, SourceStatus.PENDING, null);
        assertActionSourceStatus(SourceStatus.STALE, SourceStatus.WARMING_UP, SourceStatus.STALE,
                NOW.minus(Duration.ofMinutes(5)));

        AdminOverviewResult allNotApplicable = serviceWithAllActionSourceStatus(SourceStatus.N_A).getOverview();
        assertThat(allNotApplicable.snapshot().actionRequired())
                .isEqualTo(valid(new AdminOverviewSnapshot.ActionRequiredSummary(3, 2, 1)));
        assertThat(allNotApplicable.snapshot().actionItems().value().topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId).containsExactly(102L, 103L, 105L);

        AdminOverviewResult allNoTraffic = serviceWithAllActionSourceStatus(SourceStatus.NO_TRAFFIC).getOverview();
        // NO_TRAFFIC은 O1·O2 흐름에만 해당하고 FINAL·준비 미완료 조치는 독립 원천이므로 정상값으로 보존합니다.
        assertThat(allNoTraffic.snapshot().actionRequired())
                .isEqualTo(valid(new AdminOverviewSnapshot.ActionRequiredSummary(3, 2, 1)));
        assertThat(allNoTraffic.snapshot().actionItems().value().topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId).containsExactly(102L, 103L, 105L);
    }

    /** 명시적인 재고 최신성·미수집 상태가 O4 행과 전역 위험에 손실 없이 전달되는지 검증합니다. */
    @Test
    @DisplayName("재고 STALE WARMING_UP PENDING 상태는 Service O4 행과 전역 위험에 전파된다")
    void propagatesExplicitStockSourceStates() {
        for (SourceStatus status : List.of(SourceStatus.STALE, SourceStatus.WARMING_UP, SourceStatus.PENDING)) {
            AdminOverviewResult result = serviceWithStockStatus(status).getOverview();
            AdminOverviewSnapshot.CampaignOverview campaign = result.snapshot().campaigns().value().stream()
                    .filter(row -> row.couponId().equals(103L)).findFirst().orElseThrow();
            assertThat(campaign.stockForecast().status()).isEqualTo(status);
            assertThat(result.snapshot().stockRisk().status()).isEqualTo(status);
        }
    }

    /** 계산이 끝난 내부 값이 HTTP 변환 과정에서 누락되거나 전체 상태가 낮아지는 회귀를 방지합니다. */
    @Test
    @DisplayName("모든 운영 원천이 해석 가능하면 전체 Snapshot을 보존하고 COMPLETE로 조립한다")
    void assemblesCompleteSnapshotWithoutLosingNestedValues() {
        AdminOverviewSnapshot snapshot = completeSnapshot(validStockRisk(), validCampaigns());

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.snapshot()).isSameAs(snapshot);
        assertThat(result.overallStatus()).isEqualTo(OverallStatus.COMPLETE);
        assertThat(result.snapshot().actionRequired().value().totalCount()).isEqualTo(1);
        assertThat(result.snapshot().actionItems().value().topItems())
                .singleElement()
                .satisfies(item -> {
                    assertThat(item.couponId()).isEqualTo(17L);
                    assertThat(item.recommendedAction().code())
                            .isEqualTo(AdminOverviewSnapshot.ActionCode.QUEUE_STALLED);
                });
        assertThat(result.snapshot().campaigns().value())
                .singleElement()
                .satisfies(campaign -> {
                    assertThat(campaign.issuanceFlow().value().currentPerMinute()).isEqualTo(44.0);
                    assertThat(campaign.campaignQueueStatus().value().waitingCount()).isEqualTo(3204);
                    assertThat(campaign.stockForecast().value().remainingRatio()).isEqualTo(0.31);
                });
        assertThat(result.snapshot().customerOutcomes().value().outcomes())
                .singleElement()
                .satisfies(outcome -> {
                    assertThat(outcome.type()).isEqualTo(AdminOverviewSnapshot.CustomerOutcomeType.ISSUED);
                    assertThat(outcome.ratio()).isEqualTo(1.0);
                });
    }

    /** 일부 원천을 읽지 못했는데 나머지 정상값까지 버리거나 전체 성공으로 표시하는 회귀를 방지합니다. */
    @Test
    @DisplayName("일부 최상위 원천이 미수집이면 정상값을 유지하고 PARTIAL로 조립한다")
    void assemblesPartialWhenOneTopLevelSourceIsUnavailable() {
        AdminOverviewSnapshot snapshot = completeSnapshot(unavailable(), validCampaigns());

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.PARTIAL);
        assertThat(result.snapshot().stockRisk().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.snapshot().aggregateIssuanceRate().value().currentPerSecond()).isEqualTo(12.0);
        assertThat(result.snapshot().campaigns().value()).hasSize(1);
    }

    /** 캠페인 목록 자체는 정상이어도 내부 O1·O2·O4 원천 실패가 전체 완전성에 반영되는지 검증합니다. */
    @Test
    @DisplayName("캠페인 중첩 원천이 미수집이면 전체 상태를 PARTIAL로 조립한다")
    void assemblesPartialWhenNestedCampaignSourceIsUnavailable() {
        AdminOverviewSnapshot snapshot = completeSnapshot(
                validStockRisk(),
                validCampaignsWithUnavailableStockForecast()
        );

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.PARTIAL);
        assertThat(result.snapshot().campaigns().value().getFirst().stockForecast().status())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.snapshot().campaigns().value().getFirst().issuanceFlow().status())
                .isEqualTo(SourceStatus.VALID);
    }

    /** 적용 대상이 아닌 캠페인 원천을 장애로 오인해 전체 상태를 낮추는 회귀를 방지합니다. */
    @Test
    @DisplayName("캠페인 중첩 원천이 N_A이면 전체 완전성 계산에서 제외한다")
    void excludesNotApplicableNestedSourceFromCompleteness() {
        AdminOverviewSnapshot snapshot = completeSnapshot(
                validStockRisk(),
                valid(List.of(campaign(notApplicable())))
        );

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.COMPLETE);
        assertThat(result.snapshot().campaigns().value().getFirst().stockForecast().status())
                .isEqualTo(SourceStatus.N_A);
    }

    /** 준비 중이거나 오래된 값은 표시할 수 있지만 완전한 최신값은 아니라는 정책을 고정합니다. */
    @Test
    @DisplayName("WARMING_UP 또는 STALE 값이 있으면 값을 보존하고 PARTIAL로 조립한다")
    void preservesInterpretableButIncompleteSourceAsPartial() {
        for (SourceStatus sourceStatus : List.of(SourceStatus.WARMING_UP, SourceStatus.STALE)) {
            AdminOverviewSnapshot snapshot = completeSnapshot(
                    observed(
                            new AdminOverviewSnapshot.StockRiskSummary(1, Duration.ofMinutes(8)),
                            sourceStatus),
                    validCampaigns()
            );

            AdminOverviewResult result = service().assemble(snapshot);

            assertThat(result.overallStatus()).isEqualTo(OverallStatus.PARTIAL);
            assertThat(result.snapshot().stockRisk().value().depletionRiskCount()).isEqualTo(1);
            assertThat(result.snapshot().stockRisk().status()).isEqualTo(sourceStatus);
        }
    }

    /** 실제 요청이 없다는 확정 관측을 원천 장애로 오인하지 않도록 합니다. */
    @Test
    @DisplayName("NO_TRAFFIC은 실제 0 관측값이므로 전체 COMPLETE를 유지한다")
    void treatsNoTrafficAsCompleteObservation() {
        AdminOverviewSnapshot snapshot = completeSnapshot(
                observed(
                        new AdminOverviewSnapshot.StockRiskSummary(0, null),
                        SourceStatus.NO_TRAFFIC),
                validCampaigns()
        );

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.COMPLETE);
        assertThat(result.snapshot().stockRisk().value().depletionRiskCount()).isZero();
        assertThat(result.snapshot().stockRisk().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
    }

    /** 미수집 상태만 모인 Snapshot을 일부 성공으로 표시하는 회귀를 방지합니다. */
    @Test
    @DisplayName("해석 가능한 원천값이 하나도 없으면 UNAVAILABLE로 조립한다")
    void assemblesUnavailableWhenNoSourceHasUsableValue() {
        AdminOverviewSnapshot snapshot = unavailableSnapshot();

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.UNAVAILABLE);
        assertThat(observations(result.snapshot()))
                .allSatisfy(observation -> assertThat(observation.status())
                        .isIn(SourceStatus.PENDING, SourceStatus.UNAVAILABLE, SourceStatus.N_A));
    }

    /** 같은 원천에서 파생한 조치 결과를 별도 장애 원천처럼 중복 계산하는 회귀를 방지합니다. */
    @Test
    @DisplayName("조치 KPI와 목록 상태는 독립 원천이 아니므로 전체 완전성에서 제외한다")
    void excludesDerivedActionStatusesFromOverallCompleteness() {
        AdminOverviewSnapshot complete = completeSnapshot(validStockRisk(), validCampaigns());
        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                complete.snapshotAt(),
                unavailable(),
                complete.openingSoon(),
                complete.queueRisk(),
                complete.stockRisk(),
                complete.aggregateIssuanceRate(),
                complete.aggregateQueue(),
                complete.latencySummary(),
                complete.campaignStatusSummary(),
                unavailable(),
                complete.campaigns(),
                complete.customerOutcomes()
        );

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.COMPLETE);
        assertThat(result.snapshot().actionRequired().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.snapshot().actionItems().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 보조 HTTP 지연값만으로 핵심 운영현황을 사용할 수 있다고 판정하는 회귀를 방지합니다. */
    @Test
    @DisplayName("HTTP 지연만 정상이고 핵심 운영 원천이 모두 미수집이면 UNAVAILABLE을 유지한다")
    void doesNotPromoteLatencyOnlySnapshotToPartial() {
        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                NOW,
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                valid(new AdminOverviewSnapshot.LatencySummary(
                        Duration.ofMillis(80), Duration.ofMillis(120),
                        NOW.minus(Duration.ofMinutes(5)), NOW)),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable()
        );

        AdminOverviewResult result = service().assemble(snapshot);

        assertThat(result.overallStatus()).isEqualTo(OverallStatus.UNAVAILABLE);
    }

    /** 응답의 독립 관측 영역을 한 번씩 반환해 새 영역이 가짜 기본값으로 빠지는 것을 확인합니다. */
    private static List<AdminOverviewSnapshot.Observation<?>> observations(
            AdminOverviewSnapshot snapshot
    ) {
        return List.of(
                snapshot.actionRequired(),
                snapshot.openingSoon(),
                snapshot.queueRisk(),
                snapshot.stockRisk(),
                snapshot.aggregateIssuanceRate(),
                snapshot.aggregateQueue(),
                snapshot.latencySummary(),
                snapshot.campaignStatusSummary(),
                snapshot.actionItems(),
                snapshot.campaigns(),
                snapshot.customerOutcomes()
        );
    }

    private static AdminOverviewService service() {
        TimeProvider timeProvider = new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC));
        OverviewStatusCalculator statusCalculator = new OverviewStatusCalculator();
        return new AdminOverviewService(
                timeProvider,
                new AdminOverviewMockDataFactory(),
                new IssuanceFlowCalculator(),
                new IssuanceActionCalculator(),
                new CampaignQueueCalculator(),
                new CustomerOutcomeCalculator(),
                new StockRiskCalculator(),
                new CampaignOverviewCalculator(),
                new ConsistencyActionCalculator(),
                new OperationActionCalculator(),
                statusCalculator
        );
    }

    private static AdminOverviewService serviceWithQueueStatus(Long couponId, SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset create(Instant snapshotAt) {
                com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset base = super.create(snapshotAt);
                List<QueueInput> inputs = base.queueInputs().stream()
                        .map(input -> input.couponId().equals(couponId)
                                ? queueWithStatus(input, status) : input)
                        .toList();
                return new com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset(base.policy(),
                        base.issuanceFlowInputs(), inputs, base.outcomeInput(), base.campaigns(),
                        base.preparationActionCandidates(), base.consistencyActionContexts(), base.aggregateIssuanceRate(),
                        base.latencySummary());
            }
        });
    }

    private static AdminOverviewService serviceWithStockStatus(SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset create(Instant snapshotAt) {
                com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset base = super.create(snapshotAt);
                List<CampaignOverviewSource> campaigns = base.campaigns().stream()
                        .map(source -> source.couponId().equals(103L)
                                ? new CampaignOverviewSource(source.couponId(), source.campaignName(),
                                source.brandName(), source.status(), source.opensAt(), source.closesAt(),
                                source.engineVersion(), status.carriesValue() ? 10_000L : null,
                                status.carriesValue() ? 3_700L : null,
                                status.carriesValue() ? snapshotAt : null, status,
                                source.preparationCompleted()) : source)
                        .toList();
                return new com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset(base.policy(),
                        base.issuanceFlowInputs(), base.queueInputs(), base.outcomeInput(), campaigns,
                        base.preparationActionCandidates(), base.consistencyActionContexts(), base.aggregateIssuanceRate(),
                        base.latencySummary());
            }
        });
    }

    private static AdminOverviewService serviceWithIssuanceStatus(Long couponId, SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset create(Instant snapshotAt) {
                com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> inputs = base.issuanceFlowInputs().stream()
                        .map(input -> input.couponId().equals(couponId)
                                ? issuanceWithStatus(input, status, snapshotAt) : input)
                        .toList();
                return new com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset(base.policy(), inputs,
                        base.queueInputs(), base.outcomeInput(), base.campaigns(),
                        base.preparationActionCandidates(), base.consistencyActionContexts(), base.aggregateIssuanceRate(),
                        base.latencySummary());
            }
        });
    }

    /** O1 한 원천과 O2 한 원천만 적용 대상으로 남겨 Action 상태 합성 우선순위를 검증합니다. */
    private static AdminOverviewService serviceWithActionSourceStatuses(
            SourceStatus issuanceStatus,
            SourceStatus queueStatus
    ) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset create(Instant snapshotAt) {
                com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> issuanceInputs = base.issuanceFlowInputs().stream()
                        .map(input -> issuanceSourceInput(input,
                                input.couponId().equals(101L) ? issuanceStatus : SourceStatus.N_A, snapshotAt))
                        .toList();
                List<QueueInput> queueInputs = base.queueInputs().stream()
                        .map(input -> queueSourceInput(input,
                                input.couponId().equals(102L) ? queueStatus : SourceStatus.N_A, snapshotAt))
                        .toList();
                return new com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset(base.policy(),
                        issuanceInputs, queueInputs, base.outcomeInput(), base.campaigns(),
                        base.preparationActionCandidates(), base.consistencyActionContexts(), base.aggregateIssuanceRate(),
                        base.latencySummary());
            }
        });
    }

    /** 모든 O1·O2 원천을 같은 상태로 바꿔 전체 N_A·NO_TRAFFIC 분기를 검증합니다. */
    private static AdminOverviewService serviceWithAllActionSourceStatus(SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset create(Instant snapshotAt) {
                com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> issuanceInputs = base.issuanceFlowInputs().stream()
                        .map(input -> issuanceSourceInput(input, status, snapshotAt))
                        .toList();
                List<QueueInput> queueInputs = base.queueInputs().stream()
                        .map(input -> queueSourceInput(input, status, snapshotAt))
                        .toList();
                return new com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset(base.policy(),
                        issuanceInputs, queueInputs, base.outcomeInput(), base.campaigns(),
                        base.preparationActionCandidates(), base.consistencyActionContexts(), base.aggregateIssuanceRate(),
                        base.latencySummary());
            }
        });
    }

    private static AdminOverviewService serviceWithDataset(AdminOverviewMockDataFactory factory) {
        return new AdminOverviewService(new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC)), factory,
                new IssuanceFlowCalculator(), new IssuanceActionCalculator(), new CampaignQueueCalculator(),
                new CustomerOutcomeCalculator(),
                new StockRiskCalculator(), new CampaignOverviewCalculator(), new ConsistencyActionCalculator(),
                new OperationActionCalculator(),
                new OverviewStatusCalculator());
    }

    private static QueueInput queueWithStatus(QueueInput input, SourceStatus status) {
        if (!status.carriesValue()) {
            return new QueueInput(input.couponId(), null, null, null, null, null, null, null, status, null);
        }
        return new QueueInput(input.couponId(), input.currentWaitingCount(), input.previousWaitingCount(),
                input.admittedCount(), input.windowStart(), input.windowEnd(), input.lastAdmissionAt(),
                input.admissionStoppedStartedAt(), status, input.observedAt());
    }

    /** 테스트에서 O1 중단만 바꾸고 같은 쿠폰의 실제 관측 구간·원천 상태는 유지합니다. */
    private static IssuanceFlowInput stoppedIssuanceInput(IssuanceFlowInput input, Instant snapshotAt) {
        return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), input.stockAvailable(),
                input.windowStart(), input.windowEnd(), input.attemptedCount(), 0L,
                input.comparisonCompletedCount(), input.comparisonWindowStart(), input.comparisonWindowEnd(),
                List.of(), null, snapshotAt.minus(Duration.ofMinutes(10)), SourceStatus.VALID, snapshotAt);
    }

    /** 값 있는 최신성 상태는 더 오래된 실제 O1 관측 시각을 포함하고 값 없는 상태는 null로 만듭니다. */
    private static IssuanceFlowInput issuanceWithStatus(
            IssuanceFlowInput input,
            SourceStatus status,
            Instant snapshotAt
    ) {
        if (!status.carriesValue()) {
            return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), null, null, null,
                    null, null, null, null, null, null, null, null, status, null);
        }
        Instant observedAt = snapshotAt.minus(Duration.ofMinutes(5));
        Instant windowStart = observedAt.minus(Duration.ofMinutes(1));
        Instant comparisonWindowStart = windowStart.minus(Duration.ofMinutes(1));
        return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), true, windowStart, observedAt,
                10L, 10L, 10L, comparisonWindowStart, windowStart,
                List.of(new IssuanceFlowCalculator.IssuanceBucket(windowStart, observedAt, 10L)), observedAt,
                windowStart, status, observedAt);
    }

    /** 혼합 상태 테스트용으로 실제 O1 관측 상태와 시각을 명시적으로 만듭니다. */
    private static IssuanceFlowInput issuanceSourceInput(
            IssuanceFlowInput input,
            SourceStatus status,
            Instant snapshotAt
    ) {
        if (!status.carriesValue()) {
            return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), null, null, null,
                    null, null, null, null, null, null, null, null, status, null);
        }
        Instant observedAt = snapshotAt.minus(Duration.ofMinutes(5));
        Instant windowStart = observedAt.minus(Duration.ofMinutes(1));
        Instant comparisonWindowStart = windowStart.minus(Duration.ofMinutes(1));
        if (status == SourceStatus.NO_TRAFFIC) {
            return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), true, windowStart, observedAt,
                    0L, 0L, 0L, comparisonWindowStart, windowStart, List.of(), null, windowStart, status, observedAt);
        }
        return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), true, windowStart, observedAt,
                10L, 10L, 10L, comparisonWindowStart, windowStart,
                List.of(new IssuanceFlowCalculator.IssuanceBucket(windowStart, observedAt, 10L)), observedAt,
                windowStart, status, observedAt);
    }

    /** 혼합 상태 테스트용으로 실제 O2 관측 상태와 시각을 명시적으로 만듭니다. */
    private static QueueInput queueSourceInput(QueueInput input, SourceStatus status, Instant snapshotAt) {
        if (!status.carriesValue()) {
            return new QueueInput(input.couponId(), null, null, null, null, null, null, null, status, null);
        }
        Instant observedAt = snapshotAt.minus(Duration.ofMinutes(3));
        Instant windowStart = observedAt.minus(Duration.ofMinutes(1));
        return new QueueInput(input.couponId(), 0L, 0L, 0L, windowStart, observedAt, null, null, status, observedAt);
    }

    /** Action 값의 상태·시각을 함께 검증해 혼합 상태 우선순위가 바뀌는 회귀를 막습니다. */
    private static void assertActionSourceStatus(
            SourceStatus issuanceStatus,
            SourceStatus queueStatus,
            SourceStatus expectedStatus,
            Instant expectedObservedAt
    ) {
        AdminOverviewResult result = serviceWithActionSourceStatuses(issuanceStatus, queueStatus).getOverview();
        if (!expectedStatus.carriesValue()) {
            assertThat(result.snapshot().actionRequired())
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, expectedStatus, null));
            assertThat(result.snapshot().actionItems())
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, expectedStatus, null));
            return;
        }
        assertThat(result.snapshot().actionRequired().status()).isEqualTo(expectedStatus);
        assertThat(result.snapshot().actionItems().status()).isEqualTo(expectedStatus);
        assertThat(result.snapshot().actionRequired().value())
                .isEqualTo(new AdminOverviewSnapshot.ActionRequiredSummary(3, 2, 1));
        assertThat(result.snapshot().actionItems().value().topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId).containsExactly(102L, 103L, 105L);
        assertThat(result.snapshot().actionRequired().observedAt()).isEqualTo(expectedObservedAt);
        assertThat(result.snapshot().actionItems().observedAt()).isEqualTo(expectedObservedAt);
    }

    private static AdminOverviewSnapshot completeSnapshot(
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockRiskSummary> stockRisk,
            AdminOverviewSnapshot.Observation<List<AdminOverviewSnapshot.CampaignOverview>> campaigns
    ) {
        AdminOverviewSnapshot.OperationActionItem action = new AdminOverviewSnapshot.OperationActionItem(
                17L,
                "여름 특가",
                NOW.minus(Duration.ofHours(1)),
                Severity.CRITICAL,
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "입장 처리가 멈춰 고객 대기가 지속됩니다.",
                NOW.minus(Duration.ofMinutes(3)),
                Duration.ofMinutes(3),
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "대기열 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS)
        );
        return new AdminOverviewSnapshot(
                NOW,
                valid(new AdminOverviewSnapshot.ActionRequiredSummary(1, 1, 0)),
                valid(new AdminOverviewSnapshot.OpeningSoonSummary(1, 0)),
                valid(new AdminOverviewSnapshot.QueueRiskSummary(1, Duration.ofMinutes(9))),
                stockRisk,
                valid(new AdminOverviewSnapshot.AggregateIssuanceRate(12.0, 18.0)),
                valid(new AdminOverviewSnapshot.AggregateQueue(3204, 4.5, Duration.ofMinutes(12))),
                valid(new AdminOverviewSnapshot.LatencySummary(
                        Duration.ofMillis(80), Duration.ofMillis(120),
                        NOW.minus(Duration.ofMinutes(5)), NOW)),
                valid(new AdminOverviewSnapshot.CampaignStatusSummary(1, 1, 0)),
                valid(new AdminOverviewSnapshot.ActionItemSnapshot(1, List.of(action))),
                campaigns,
                valid(new AdminOverviewSnapshot.CustomerOutcomeSummary(
                        NOW.minus(Duration.ofMinutes(5)),
                        NOW,
                        1,
                        List.of(new AdminOverviewSnapshot.CustomerOutcome(
                                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED,
                                1,
                                1.0,
                                "정상 발급"))))
        );
    }

    private static AdminOverviewSnapshot unavailableSnapshot() {
        return new AdminOverviewSnapshot(
                NOW,
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable()
        );
    }

    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockRiskSummary> validStockRisk() {
        return valid(new AdminOverviewSnapshot.StockRiskSummary(1, Duration.ofMinutes(8)));
    }

    private static AdminOverviewSnapshot.Observation<List<AdminOverviewSnapshot.CampaignOverview>> validCampaigns() {
        return valid(List.of(campaign(valid(new AdminOverviewSnapshot.StockForecast(
                4650, 15000, 0.31, Duration.ofMinutes(8))))));
    }

    private static AdminOverviewSnapshot.Observation<List<AdminOverviewSnapshot.CampaignOverview>>
    validCampaignsWithUnavailableStockForecast() {
        return valid(List.of(campaign(unavailable())));
    }

    private static AdminOverviewSnapshot.CampaignOverview campaign(
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast> stockForecast
    ) {
        return new AdminOverviewSnapshot.CampaignOverview(
                1,
                17L,
                "여름 특가",
                "쿠폰야호",
                CouponStatus.OPEN,
                NOW.minus(Duration.ofHours(1)),
                NOW.plus(Duration.ofHours(2)),
                Severity.CRITICAL,
                valid(new AdminOverviewSnapshot.IssuanceFlow(
                        44.0,
                        NOW.minus(Duration.ofMinutes(5)),
                        NOW,
                        List.of(new AdminOverviewSnapshot.IssuanceRatePoint(NOW, 44.0)),
                        AdminOverviewSnapshot.IssuanceFlowState.NORMAL,
                        Duration.ofMinutes(1))),
                valid(new AdminOverviewSnapshot.CampaignQueueStatus(
                        3204,
                        AdminOverviewSnapshot.TrendDirection.INCREASING,
                        180,
                        4.5,
                        Duration.ofMinutes(12),
                        AdminOverviewSnapshot.CampaignQueueAssessment.GUIDANCE_THRESHOLD_EXCEEDED)),
                stockForecast,
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "고객 대기 증가",
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "대기열 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS)
        );
    }

    private static AdminOverviewSnapshot.CampaignOverview campaignForCoupon(AdminOverviewResult result, long couponId) {
        return result.snapshot().campaigns().value().stream()
                .filter(row -> row.couponId().equals(couponId))
                .findFirst()
                .orElseThrow();
    }

    private static <T> AdminOverviewSnapshot.Observation<T> valid(T value) {
        return observed(value, SourceStatus.VALID);
    }

    private static <T> AdminOverviewSnapshot.Observation<T> observed(
            T value,
            SourceStatus status
    ) {
        return new AdminOverviewSnapshot.Observation<>(value, status, NOW);
    }

    private static <T> AdminOverviewSnapshot.Observation<T> unavailable() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }

    private static <T> AdminOverviewSnapshot.Observation<T> notApplicable() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.N_A, null);
    }

    /** 실제 TimeProvider 동작을 보존하면서 요청 시간 경계 호출 횟수와 순서를 기록합니다. */
    private static final class RecordingTimeProvider extends TimeProvider {

        private final List<String> events;

        private RecordingTimeProvider(List<String> events) {
            super(Clock.fixed(NOW, ZoneOffset.UTC));
            this.events = events;
        }

        @Override
        public Instant instant() {
            events.add("time");
            return super.instant();
        }
    }

    /** 실제 Mock Factory의 Dataset 생성 결과를 보존하면서 호출 횟수와 순서를 기록합니다. */
    private static final class RecordingMockDataFactory extends AdminOverviewMockDataFactory {

        private final List<String> events;
        private int createCount;

        private RecordingMockDataFactory(List<String> events) {
            this.events = events;
        }

        @Override
        public com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset create(Instant snapshotAt) {
            createCount++;
            events.add("factory");
            return super.create(snapshotAt);
        }
    }

    /** 실제 O1 계산 결과를 유지하는 호출 계수기입니다. */
    private static final class RecordingIssuanceFlowCalculator extends IssuanceFlowCalculator {

        private final List<String> events;
        private int calculateCount;
        private IssuanceFlowCalculation result;

        private RecordingIssuanceFlowCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public IssuanceFlowCalculation calculate(OverviewCalculationPolicy policy, List<IssuanceFlowInput> inputs) {
            calculateCount++;
            events.add("issuance");
            result = super.calculate(policy, inputs);
            return result;
        }
    }

    /** 실제 O1 조치 후보를 보존하면서 후보 계산 호출 횟수와 순서를 기록합니다. */
    private static final class RecordingIssuanceActionCalculator extends IssuanceActionCalculator {

        private final List<String> events;
        private int calculateCount;

        private RecordingIssuanceActionCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public List<AdminOverviewSnapshot.OperationActionItem> calculate(
                Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows
        ) {
            calculateCount++;
            events.add("issuanceAction");
            return super.calculate(issuanceFlows);
        }
    }

    /** 실제 O2 계산 결과를 유지하는 호출 계수기입니다. */
    private static final class RecordingQueueCalculator extends CampaignQueueCalculator {

        private final List<String> events;
        private int calculateCount;
        private QueueCalculation result;

        private RecordingQueueCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public QueueCalculation calculate(OverviewCalculationPolicy policy, List<QueueInput> inputs) {
            calculateCount++;
            events.add("queue");
            result = super.calculate(policy, inputs);
            return result;
        }
    }

    /** 실제 O3 계산 결과를 유지하는 호출 계수기입니다. */
    private static final class RecordingOutcomeCalculator extends CustomerOutcomeCalculator {

        private final List<String> events;
        private int calculateCount;
        private OutcomeCalculation result;

        private RecordingOutcomeCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public OutcomeCalculation calculate(OutcomeInput input) {
            calculateCount++;
            events.add("outcome");
            result = super.calculate(input);
            return result;
        }
    }

    /** 실제 O4 계산 결과를 유지하고 같은 O1 Observation이 전달됐는지 확인할 입력을 기록합니다. */
    private static final class RecordingStockRiskCalculator extends StockRiskCalculator {

        private final List<String> events;
        private int calculateCount;
        private List<StockInput> inputs;
        private StockRiskCalculation result;

        private RecordingStockRiskCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public StockRiskCalculation calculate(OverviewCalculationPolicy policy, List<StockInput> inputs) {
            calculateCount++;
            events.add("stock");
            this.inputs = List.copyOf(inputs);
            result = super.calculate(policy, inputs);
            return result;
        }
    }

    /** 실제 Campaign 조립 결과를 유지하는 호출 계수기입니다. */
    private static final class RecordingCampaignOverviewCalculator extends CampaignOverviewCalculator {

        private final List<String> events;
        private int calculateCount;

        private RecordingCampaignOverviewCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public CampaignCalculation calculate(Instant snapshotAt, List<CampaignOverviewSource> campaigns,
                Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows,
                Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus>> queueStatuses,
                Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast>> stockForecasts,
                Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeActions) {
            calculateCount++;
            events.add("campaign");
            return super.calculate(snapshotAt, campaigns, issuanceFlows, queueStatuses, stockForecasts,
                    representativeActions);
        }
    }

    /** 실제 FINAL 조치 결과를 유지하며 문맥별 호출 횟수를 기록합니다. */
    private static final class RecordingConsistencyActionCalculator extends ConsistencyActionCalculator {

        private final List<String> events;
        private final List<ConsistencyActionContext> contexts = new ArrayList<>();
        private int calculateCount;

        private RecordingConsistencyActionCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public List<AdminOverviewSnapshot.OperationActionItem> calculate(ConsistencyActionContext context) {
            calculateCount++;
            contexts.add(context);
            events.add("consistency");
            return super.calculate(context);
        }
    }

    /** 실제 Action 집계 결과를 유지하는 호출 계수기입니다. */
    private static final class RecordingActionCalculator extends OperationActionCalculator {

        private final List<String> events;
        private int calculateCount;

        private RecordingActionCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public ActionCalculation calculate(List<AdminOverviewSnapshot.OperationActionItem> decisions) {
            calculateCount++;
            events.add("action");
            return super.calculate(decisions);
        }
    }

    /** 실제 전체 상태 계산 결과를 유지하는 호출 계수기입니다. */
    private static final class RecordingOverviewStatusCalculator extends OverviewStatusCalculator {

        private final List<String> events;
        private int calculateCount;

        private RecordingOverviewStatusCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public OverallStatus calculate(AdminOverviewSnapshot snapshot) {
            calculateCount++;
            events.add("status");
            return super.calculate(snapshot);
        }
    }
}

package com.kafkick.core.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataset;
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
import com.kafkick.core.admin.overview.observation.CampaignObservationTarget;
import com.kafkick.core.admin.overview.observation.OverviewObservationData;
import com.kafkick.core.admin.overview.observation.OverviewObservationRequest;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/** 실 O1·O3·지연과 Mock 캠페인·O2·O4·FINAL을 함께 조립하는 Overview Service를 검증합니다. */
class AdminOverviewServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-20T03:15:00Z");

    /** API 전용 원천을 Batch에 강제하지 않도록 Core Service는 전역 Spring 컴포넌트가 아닙니다. */
    @Test
    @DisplayName("AdminOverviewService는 Core 전역 컴포넌트가 아니다")
    void remainsTechnicalNeutralWithoutSpringComponentOwnership() {
        assertThat(AnnotatedElementUtils.hasAnnotation(AdminOverviewService.class, Component.class)).isFalse();
    }

    /** 연결된 O1·O3·지연과 Mock O2·O4·FINAL이 같은 Snapshot으로 조립되는지 검증합니다. */
    @Test
    @DisplayName("관측 O1 O3 지연과 Mock O2 O4 FINAL을 PARTIAL 운영현황으로 조립한다")
    void assemblesObservedAndMockBoundariesAsPartialOverview() {
        AdminOverviewService service = service();

        AdminOverviewResult result = service.getOverview();
        AdminOverviewSnapshot snapshot = result.snapshot();

        assertThat(snapshot.snapshotAt()).isEqualTo(NOW);
        assertThat(result.overallStatus()).isEqualTo(OverallStatus.PARTIAL);
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
                    assertThat(campaign.issuanceFlow().value().currentPerMinute()).isEqualTo(49.0);
                    assertThat(campaign.stockForecast().value())
                        .isEqualTo(new AdminOverviewSnapshot.StockForecast(
                                350L, 7_000L, 0.05, Duration.ofSeconds(429)));
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
                    assertThat(observation.value()).isNull();
                    assertThat(observation.status()).isEqualTo(SourceStatus.PENDING);
                    assertThat(observation.observedAt()).isNull();
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

    /** 실관측 O1·O3·지연과 원천 aggregate 상태가 Mock 값 대신 Snapshot에 직접 반영되는지 검증합니다. */
    @Test
    @DisplayName("관측 O1 O3 지연과 aggregate PENDING을 fallback 없이 조립한다")
    void assemblesObservedIssuanceOutcomeLatencyAndAggregateStatus() {
        AdminOverviewResult result = serviceWithObservationSource(distinctObservationSource()).getOverview();
        AdminOverviewSnapshot snapshot = result.snapshot();

        assertThat(campaignForCoupon(result, 102L).issuanceFlow().value().currentPerMinute()).isEqualTo(5d);
        assertThat(snapshot.customerOutcomes().value().totalCount()).isEqualTo(2d);
        assertThat(snapshot.customerOutcomes().value().outcomes())
                .filteredOn(outcome -> outcome.type() == AdminOverviewSnapshot.CustomerOutcomeType.ISSUED)
                .singleElement()
                .extracting(AdminOverviewSnapshot.CustomerOutcome::count)
                .isEqualTo(2d);
        assertThat(snapshot.latencySummary())
                .isEqualTo(new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(
                                Duration.ofMillis(11), Duration.ofMillis(22),
                                NOW.minus(Duration.ofMinutes(5)), NOW),
                        SourceStatus.VALID,
                        NOW));
        assertThat(snapshot.aggregateIssuanceRate()).isEqualTo(pendingObservation());
    }

    /** O2·O4·FINAL·캠페인 Mock 경계가 실 O1 sentinel 때문에 바뀌지 않는지 검증합니다. */
    @Test
    @DisplayName("실 O1과 별도인 Mock O1으로 O4를 계산하고 O2 FINAL 캠페인을 유지한다")
    void keepsMockQueueStockFinalAndCampaignBoundaries() {
        AdminOverviewResult result = serviceWithObservationSource(distinctObservationSource()).getOverview();

        assertThat(result.snapshot().aggregateQueue().value())
                .isEqualTo(new AdminOverviewSnapshot.AggregateQueue(3_388L, 4.6, null));
        assertThat(campaignForCoupon(result, 102L).stockForecast().value())
                .isEqualTo(new AdminOverviewSnapshot.StockForecast(
                        350L, 7_000L, 0.05, Duration.ofSeconds(429)));
        assertThat(campaignForCoupon(result, 102L).recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE);
        assertThat(result.snapshot().campaigns().value())
                .extracting(AdminOverviewSnapshot.CampaignOverview::couponId)
                .containsExactly(101L, 102L, 103L, 105L, 104L, 106L);
    }

    /** 실 O1에서 파생된 한 대표 조치를 KPI·목록·캠페인 행이 함께 사용하는지 검증합니다. */
    @Test
    @DisplayName("실 STOPPED O1 대표 조치를 Action KPI 목록과 캠페인 행이 공유한다")
    void sharesObservedStoppedIssuanceRepresentativeAcrossActionSurfaces() {
        AdminOverviewResult result = serviceWithObservationSource(distinctObservationSource()).getOverview();

        AdminOverviewSnapshot.OperationActionItem item = result.snapshot().actionItems().value().topItems().stream()
                .filter(action -> action.couponId().equals(103L))
                .findFirst()
                .orElseThrow();
        assertThat(result.snapshot().actionRequired().value())
                .isEqualTo(new AdminOverviewSnapshot.ActionRequiredSummary(4, 3, 1));
        assertThat(item.recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED);
        assertThat(campaignForCoupon(result, 103L).issuanceFlow().value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.STOPPED);
        assertThat(campaignForCoupon(result, 103L).recommendedAction()).isSameAs(item.recommendedAction());
    }

    /** 값 없는 실관측 상태가 Mock O1·O3·지연 값으로 보정되지 않는지 검증합니다. */
    @Test
    @DisplayName("O1 O3 지연 미관측 상태를 유지하고 O2 O4 FINAL만 Mock으로 계산한다")
    void preservesUnavailableObservationsWithoutMockFallback() {
        AdminOverviewResult result = serviceWithObservationSource(unavailableObservationSource()).getOverview();
        AdminOverviewSnapshot snapshot = result.snapshot();

        assertThat(campaignForCoupon(result, 102L).issuanceFlow()).isEqualTo(pendingObservation());
        assertThat(snapshot.customerOutcomes()).isEqualTo(pendingObservation());
        assertThat(snapshot.latencySummary()).isEqualTo(unavailable());
        assertThat(snapshot.aggregateIssuanceRate()).isEqualTo(pendingObservation());
        assertThat(snapshot.actionRequired()).isEqualTo(pendingObservation());
        assertThat(snapshot.actionItems()).isEqualTo(pendingObservation());
        assertThat(snapshot.aggregateQueue().status()).isEqualTo(SourceStatus.VALID);
        assertThat(campaignForCoupon(result, 102L).stockForecast().status()).isEqualTo(SourceStatus.VALID);
        assertThat(campaignForCoupon(result, 102L).recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE);
    }

    /** 한 Snapshot 요청이 정책·모집단과 재고 값·상태·시각을 보존해 원천을 한 번 호출하는지 검증합니다. */
    @Test
    @DisplayName("관측 요청은 한 번이며 OPEN 재고 관측과 비OPEN N_A null을 전달한다")
    void observesOnceWithDatasetPolicyAndCampaignTargets() {
        AdminOverviewMockDataFactory factory = new AdminOverviewMockDataFactory() {
            @Override
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<CampaignOverviewSource> campaigns = base.campaigns().stream()
                        .map(campaign -> campaign.couponId().equals(102L)
                                ? campaignWithStock(campaign, 7_000L, 7_000L, snapshotAt) : campaign)
                        .toList();
                return withCampaigns(base, campaigns);
            }
        };
        List<OverviewObservationRequest> requests = new ArrayList<>();
        OverviewObservationSource source = request -> {
            requests.add(request);
            return mockObservationData(request, factory.create(request.snapshotAt()));
        };

        serviceWithSources(factory, source).getOverview();

        assertThat(requests).singleElement().satisfies(request -> {
            assertThat(request.snapshotAt()).isEqualTo(NOW);
            assertThat(request.policy()).isEqualTo(factory.create(NOW).policy());
            assertThat(request.campaignTargets()).containsExactly(
                    new CampaignObservationTarget(
                            101L, CouponStatus.OPEN, true, SourceStatus.VALID, NOW),
                    new CampaignObservationTarget(
                            102L, CouponStatus.OPEN, false, SourceStatus.VALID, NOW),
                    new CampaignObservationTarget(
                            103L, CouponStatus.OPEN, true, SourceStatus.VALID, NOW),
                    new CampaignObservationTarget(
                            104L, CouponStatus.SCHEDULED, null, SourceStatus.N_A, null),
                    new CampaignObservationTarget(
                            105L, CouponStatus.SCHEDULED, null, SourceStatus.N_A, null),
                    new CampaignObservationTarget(
                            106L, CouponStatus.CLOSED, null, SourceStatus.N_A, null));
        });
    }

    /** 관측 원천이 다른 Snapshot·정책·모집단의 응답을 현재 요청에 섞는 것을 Service에서 거부합니다. */
    @Test
    @DisplayName("관측 응답 request가 보낸 request와 다르면 Overview 조립을 거부한다")
    void rejectsObservationDataForDifferentRequest() {
        AdminOverviewMockDataFactory factory = new AdminOverviewMockDataFactory();
        OverviewObservationSource mismatched = request -> {
            Instant previousSnapshot = request.snapshotAt().minusSeconds(1);
            AdminOverviewMockDataset previousDataset = factory.create(previousSnapshot);
            List<CampaignObservationTarget> previousTargets = request.campaignTargets().stream()
                    .map(target -> new CampaignObservationTarget(
                            target.couponId(), target.campaignStatus(), target.stockAvailable(),
                            target.stockStatus(), target.stockStatus().carriesValue()
                                    ? previousSnapshot : null))
                    .toList();
            OverviewObservationRequest previousRequest = new OverviewObservationRequest(
                    previousSnapshot, previousTargets, request.policy());
            return mockObservationData(previousRequest, previousDataset);
        };

        assertThatThrownBy(() -> serviceWithSources(factory, mismatched).getOverview())
                .isInstanceOfSatisfying(BusinessException.class, failure -> {
                    assertThat(failure.getErrorCode().getStatus()).isEqualTo(500);
                    assertThat(failure.getErrorCode().getCode()).isEqualTo("OVERVIEW-001");
                    assertThat(failure.getErrorCode().getMessage())
                            .isEqualTo("운영현황 관측 결과를 처리할 수 없습니다.");
                    assertThat(failure).hasMessageContaining("관측 응답 request");
                });
    }

    /**
     * 한 요청에서 issuance 계산기만 실 O1·Mock O4용으로 두 번, 나머지 협력자는 한 번씩 호출하고
     * Calculator가 만든 Observation 객체를 새로 감싸지 않고 재사용하는지 검증합니다.
     */
    @Test
    @DisplayName("Service는 issuance만 두 번 계산하고 나머지는 한 번 호출해 Observation을 조립한다")
    void invokesCollaboratorsOnceAndPreservesCalculatedObservationIdentity() {
        List<String> events = new ArrayList<>();
        RecordingTimeProvider timeProvider = new RecordingTimeProvider(events);
        RecordingMockDataFactory factory = new RecordingMockDataFactory(events);
        RecordingOverviewObservationSource observation = new RecordingOverviewObservationSource(events);
        RecordingIssuanceFlowCalculator issuance = new RecordingIssuanceFlowCalculator(events);
        RecordingIssuanceActionCalculator issuanceAction = new RecordingIssuanceActionCalculator(events);
        RecordingQueueCalculator queue = new RecordingQueueCalculator(events);
        RecordingOutcomeCalculator outcome = new RecordingOutcomeCalculator(events);
        RecordingStockRiskCalculator stock = new RecordingStockRiskCalculator(events);
        RecordingCampaignOverviewCalculator campaign = new RecordingCampaignOverviewCalculator(events);
        RecordingConsistencyActionCalculator consistency = new RecordingConsistencyActionCalculator(events);
        RecordingActionCalculator action = new RecordingActionCalculator(events);
        RecordingOverviewStatusCalculator status = new RecordingOverviewStatusCalculator(events);
        AdminOverviewService service = new AdminOverviewService(
                timeProvider, factory, observation, issuance, issuanceAction, queue, outcome,
                stock, campaign, consistency, action, status);

        AdminOverviewSnapshot snapshot = service.getOverview().snapshot();

        assertThat(events).containsExactly(
                "time", "factory", "observation", "issuance", "issuanceAction", "queue", "outcome", "issuance",
                "stock", "consistency", "consistency", "consistency", "action", "campaign", "status");
        assertThat(factory.createCount).isEqualTo(1);
        assertThat(observation.observeCount).isEqualTo(1);
        assertThat(issuance.calculateCount).isEqualTo(2);
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
                .isSameAs(issuance.mockResult.issuanceFlows().get(101L));
        assertThat(snapshot.campaigns().value().stream()
                .filter(row -> row.couponId().equals(101L))
                .findFirst()
                .orElseThrow()
                .issuanceFlow())
                .isSameAs(issuance.observedResult.issuanceFlows().get(101L));
        assertThat(snapshot.queueRisk()).isSameAs(queue.result.queueRisk());
        assertThat(snapshot.aggregateQueue()).isSameAs(queue.result.aggregateQueue());
        assertThat(snapshot.stockRisk()).isSameAs(stock.result.stockRisk());
        assertThat(snapshot.customerOutcomes()).isSameAs(outcome.result.customerOutcomes());
        assertThat(snapshot.aggregateIssuanceRate()).isSameAs(observation.result.aggregateIssuanceRate());
        assertThat(snapshot.latencySummary()).isSameAs(observation.result.latencySummary());
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
            assertThat(result.snapshot().actionRequired().status()).as("status=%s", status).isEqualTo(status);
            assertThat(result.snapshot().actionItems().status()).as("status=%s", status).isEqualTo(status);
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
            assertThat(result.snapshot().actionRequired().status()).as("status=%s", status)
                    .isEqualTo(SourceStatus.VALID);
            assertThat(result.snapshot().actionItems().status()).as("status=%s", status)
                    .isEqualTo(SourceStatus.VALID);
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
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> issuanceInputs = base.issuanceFlowInputs().stream()
                        .map(input -> input.couponId().equals(103L)
                                ? stoppedIssuanceInput(input, snapshotAt) : input)
                        .toList();
                return withIssuanceFlowInputs(base, issuanceInputs);
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

    /** raw scrape 오차가 같은 심각도의 더 이른 FINAL 후보보다 O1을 앞세우는 회귀를 막습니다. */
    @Test
    @DisplayName("평가 timeline의 O1 detectedAt으로 동일 CRITICAL 대표 조치를 선택한다")
    void selectsSameSeverityRepresentativeUsingEvaluationTimelineDetectedAt() {
        AdminOverviewResult result = serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> issuanceInputs = base.issuanceFlowInputs().stream()
                        .map(input -> input.couponId().equals(103L)
                                ? stoppedIssuanceInput(
                                input, snapshotAt, snapshotAt.minus(Duration.ofMinutes(1))) : input)
                        .toList();
                List<ConsistencyActionContext> contexts = base.consistencyActionContexts().stream()
                        .map(context -> context.couponId().equals(103L)
                                ? new ConsistencyActionContext(
                                context.couponId(), context.campaignName(), context.opensAt(),
                                snapshotAt.minus(Duration.ofMinutes(10)).minusSeconds(30),
                                context.engineVersion(), context.evaluation()) : context)
                        .toList();
                return new AdminOverviewMockDataset(
                        base.policy(), issuanceInputs, base.queueInputs(), base.outcomeInput(),
                        base.campaigns(), base.preparationActionCandidates(), contexts,
                        base.aggregateIssuanceRate(), base.latencySummary());
            }
        }).getOverview();

        AdminOverviewSnapshot.OperationActionItem representative = result.snapshot().actionItems().value()
                .topItems().stream()
                .filter(action -> action.couponId().equals(103L))
                .findFirst()
                .orElseThrow();
        assertThat(campaignForCoupon(result, 103L).issuanceFlow().value().stateDuration())
                .isEqualTo(Duration.ofMinutes(10));
        assertThat(representative.detectedAt())
                .isEqualTo(NOW.minus(Duration.ofMinutes(10)).minusSeconds(30));
        assertThat(representative.recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE);
        assertThat(campaignForCoupon(result, 103L).recommendedAction()).isSameAs(
                representative.recommendedAction());
    }

    /** O1 원천의 값 없음·최신성 상태가 O2만으로 만든 정상 조치 KPI를 덮는지 검증합니다. */
    @Test
    @DisplayName("O1 원천 상태는 Action KPI 목록의 완전성과 가장 오래된 관측 시각에 반영된다")
    void propagatesIssuanceCompletenessToActionObservations() {
        for (SourceStatus status : List.of(SourceStatus.STALE, SourceStatus.WARMING_UP)) {
            AdminOverviewResult result = serviceWithIssuanceStatus(102L, status).getOverview();

            assertThat(result.snapshot().actionRequired().status()).as("status=%s", status).isEqualTo(status);
            assertThat(result.snapshot().actionItems().status()).as("status=%s", status).isEqualTo(status);
            assertThat(result.snapshot().actionRequired().value().totalCount()).isEqualTo(4L);
            assertThat(result.snapshot().actionItems().value().topItems())
                    .extracting(AdminOverviewSnapshot.OperationActionItem::couponId)
                    .containsExactly(101L, 102L, 103L, 105L);
            assertThat(result.snapshot().actionRequired().observedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(5)));
            assertThat(result.snapshot().actionItems().observedAt()).isEqualTo(NOW.minus(Duration.ofMinutes(5)));
        }
        for (SourceStatus status : List.of(SourceStatus.PENDING, SourceStatus.UNAVAILABLE)) {
            AdminOverviewResult result = serviceWithIssuanceStatus(102L, status).getOverview();

            assertThat(result.snapshot().actionRequired()).as("status=%s", status)
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, status, null));
            assertThat(result.snapshot().actionItems()).as("status=%s", status)
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
        assertThat(allNoTraffic.snapshot().actionRequired().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(allNoTraffic.snapshot().actionRequired().observedAt())
                .isEqualTo(NOW.minus(Duration.ofMinutes(5)));
        assertThat(allNoTraffic.snapshot().actionItems().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(allNoTraffic.snapshot().actionItems().observedAt())
                .isEqualTo(NOW.minus(Duration.ofMinutes(5)));
        assertThat(allNoTraffic.snapshot().actionItems().value().topItems())
                .extracting(AdminOverviewSnapshot.OperationActionItem::couponId).containsExactly(102L, 103L, 105L);
    }

    /** 정상 Action 값도 조립 시각이 아닌 실제 O1·O2 원천 중 가장 오래된 시각을 쓰는지 검증합니다. */
    @Test
    @DisplayName("Action VALID 값은 가장 오래된 실제 원천 관측 시각을 보존한다")
    void preservesOldestObservedAtForValidActionSources() {
        assertActionSourceStatus(SourceStatus.VALID, SourceStatus.VALID, SourceStatus.VALID,
                NOW.minus(Duration.ofMinutes(5)));
    }

    /** 값 있는 재고 최신성과 값 없는 재고 상태를 O1·O4에 각각 손실 없이 전달합니다. */
    @Test
    @DisplayName("OPEN 재고 값 상태와 PENDING UNAVAILABLE을 O1 O4에 전파한다")
    void propagatesExplicitStockSourceStates() {
        for (SourceStatus status : List.of(SourceStatus.STALE, SourceStatus.WARMING_UP)) {
            AdminOverviewResult result = serviceWithStockStatus(status).getOverview();
            AdminOverviewSnapshot.CampaignOverview campaign = result.snapshot().campaigns().value().stream()
                    .filter(row -> row.couponId().equals(103L)).findFirst().orElseThrow();
            assertThat(campaign.issuanceFlow().status()).as("O1 status=%s", status).isEqualTo(status);
            assertThat(campaign.stockForecast().status()).as("status=%s", status).isEqualTo(status);
            assertThat(result.snapshot().stockRisk().status()).as("status=%s", status).isEqualTo(status);
            assertThat(result.snapshot().actionItems().value().topItems())
                    .filteredOn(action -> action.couponId().equals(103L))
                    .noneMatch(action -> action.recommendedAction().code()
                            == AdminOverviewSnapshot.ActionCode.ISSUANCE_STOPPED);
        }
        for (SourceStatus status : List.of(SourceStatus.PENDING, SourceStatus.UNAVAILABLE)) {
            AdminOverviewResult result = serviceWithStockStatus(status).getOverview();
            AdminOverviewSnapshot.CampaignOverview campaign = result.snapshot().campaigns().value().stream()
                    .filter(row -> row.couponId().equals(103L)).findFirst().orElseThrow();

            assertThat(campaign.issuanceFlow()).as("O1 status=%s", status)
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, status, null));
            assertThat(campaign.stockForecast()).as("O4 status=%s", status)
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, status, null));
            assertThat(result.snapshot().stockRisk()).as("global O4 status=%s", status)
                    .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, status, null));
            assertThat(result.snapshot().customerOutcomes().status()).isEqualTo(SourceStatus.VALID);
            assertThat(result.snapshot().aggregateQueue().status()).isEqualTo(SourceStatus.VALID);
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

    /** 기본 Mock 모집단을 기술 중립 관측 응답으로 바꿔 기존 계산기 회귀 테스트에 제공합니다. */
    private static OverviewObservationSource mockObservationSource(AdminOverviewMockDataFactory factory) {
        return request -> mockObservationData(request, factory.create(request.snapshotAt()));
    }

    /** 요청 target의 재고 의미를 보존하면서 Mock Dataset을 테스트 전용 관측 묶음으로 변환합니다. */
    private static OverviewObservationData mockObservationData(
            OverviewObservationRequest request,
            AdminOverviewMockDataset dataset
    ) {
        Map<Long, CampaignObservationTarget> targets = request.campaignTargets().stream()
                .collect(java.util.stream.Collectors.toMap(CampaignObservationTarget::couponId, target -> target));
        List<IssuanceFlowInput> issuanceInputs = dataset.issuanceFlowInputs().stream()
                .map(input -> withTargetStock(input, targets.get(input.couponId())))
                .toList();
        return new OverviewObservationData(
                request,
                issuanceInputs,
                dataset.outcomeInput(),
                pendingObservation(),
                dataset.latencySummary());
    }

    /** Mock과 구별되는 실 O1·O3·지연 sentinel과 STOPPED 판정을 반환합니다. */
    private static OverviewObservationSource distinctObservationSource() {
        AdminOverviewMockDataFactory fixtureFactory = new AdminOverviewMockDataFactory();
        return request -> {
            AdminOverviewMockDataset dataset = fixtureFactory.create(request.snapshotAt());
            Map<Long, CampaignObservationTarget> targets = request.campaignTargets().stream()
                    .collect(java.util.stream.Collectors.toMap(
                            CampaignObservationTarget::couponId, target -> target));
            List<IssuanceFlowInput> issuanceInputs = dataset.issuanceFlowInputs().stream()
                    .map(input -> withTargetStock(input, targets.get(input.couponId())))
                    .map(input -> input.couponId().equals(102L)
                            ? withCurrentCompletedCount(input, 5d)
                            : input.couponId().equals(103L)
                            ? stoppedIssuanceInput(input, request.snapshotAt()) : input)
                    .toList();
            OutcomeInput outcomeInput = new OutcomeInput(
                    request.snapshotAt().minus(Duration.ofMinutes(5)),
                    request.snapshotAt(),
                    List.of(new CustomerOutcomeCalculator.OutcomeCount(
                            AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 2d)),
                    SourceStatus.VALID,
                    request.snapshotAt());
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latency =
                    new AdminOverviewSnapshot.Observation<>(
                            new AdminOverviewSnapshot.LatencySummary(
                                    Duration.ofMillis(11), Duration.ofMillis(22),
                                    request.snapshotAt().minus(Duration.ofMinutes(5)), request.snapshotAt()),
                            SourceStatus.VALID,
                            request.snapshotAt());
            return new OverviewObservationData(
                    request, issuanceInputs, outcomeInput, pendingObservation(), latency);
        };
    }

    /** O1·O3·aggregate는 PENDING, 지연은 UNAVAILABLE인 원천을 만들어 fallback 금지를 검증합니다. */
    private static OverviewObservationSource unavailableObservationSource() {
        return request -> {
            List<IssuanceFlowInput> issuanceInputs = request.campaignTargets().stream()
                    .map(target -> new IssuanceFlowInput(
                            target.couponId(), target.campaignStatus(), null,
                            null, null, null, null, null, null, null, null, null,
                            null, null, null, SourceStatus.PENDING, null))
                    .toList();
            OutcomeInput outcomeInput = new OutcomeInput(
                    null, null, null, SourceStatus.PENDING, null);
            return new OverviewObservationData(
                    request, issuanceInputs, outcomeInput, pendingObservation(), unavailable());
        };
    }

    /** O1 입력에 요청 재고의 값 또는 값 없는 상태를 보존합니다. */
    private static IssuanceFlowInput withTargetStock(
            IssuanceFlowInput input,
            CampaignObservationTarget target
    ) {
        if (!input.sourceStatus().carriesValue() || input.campaignStatus() != CouponStatus.OPEN) {
            return input;
        }
        if (!target.stockStatus().carriesValue()) {
            return new IssuanceFlowInput(
                    input.couponId(), input.campaignStatus(), null,
                    null, null, null, null, null, null, null,
                    null, null, List.of(), null, null, target.stockStatus(), null);
        }
        SourceStatus sourceStatus = input.sourceStatus();
        if (target.stockStatus() == SourceStatus.STALE) {
            sourceStatus = SourceStatus.STALE;
        } else if (target.stockStatus() == SourceStatus.WARMING_UP
                && sourceStatus != SourceStatus.STALE) {
            sourceStatus = SourceStatus.WARMING_UP;
        } else if (target.stockStatus() == SourceStatus.NO_TRAFFIC
                && sourceStatus == SourceStatus.VALID) {
            sourceStatus = input.attemptedCount() > 0d || input.completedCount() > 0d
                    ? SourceStatus.WARMING_UP : SourceStatus.NO_TRAFFIC;
        }
        Instant observedAt = input.observedAt().isBefore(target.stockObservedAt())
                ? input.observedAt() : target.stockObservedAt();
        return new IssuanceFlowInput(
                input.couponId(), input.campaignStatus(), target.stockAvailable(),
                input.windowStart(), input.windowEnd(), input.trendWindowStart(), input.trendWindowEnd(),
                input.attemptedCount(), input.completedCount(), input.comparisonCompletedCount(),
                input.comparisonWindowStart(), input.comparisonWindowEnd(), input.buckets(), input.lastCompletedAt(),
                input.conditionStartedAt(), sourceStatus, observedAt);
    }

    /** 현재 완료 sentinel만 바꿔 실 O1과 O4용 Mock O1 Map의 분리를 검증합니다. */
    private static IssuanceFlowInput withCurrentCompletedCount(IssuanceFlowInput input, double completedCount) {
        return new IssuanceFlowInput(
                input.couponId(), input.campaignStatus(), input.stockAvailable(),
                input.windowStart(), input.windowEnd(), input.trendWindowStart(), input.trendWindowEnd(),
                input.attemptedCount(), completedCount, input.comparisonCompletedCount(),
                input.comparisonWindowStart(), input.comparisonWindowEnd(), input.buckets(), input.lastCompletedAt(),
                input.conditionStartedAt(), input.sourceStatus(), input.observedAt());
    }

    /** 기존 캠페인의 값 있는 재고 수량만 바꿔 target 잔량 경계를 만듭니다. */
    private static CampaignOverviewSource campaignWithStock(
            CampaignOverviewSource campaign,
            long totalQuantity,
            long activeCount,
            Instant observedAt
    ) {
        return new CampaignOverviewSource(
                campaign.couponId(), campaign.campaignName(), campaign.brandName(), campaign.status(),
                campaign.opensAt(), campaign.closesAt(), campaign.engineVersion(), totalQuantity, activeCount,
                observedAt, SourceStatus.VALID, campaign.preparationCompleted());
    }

    /** 지정한 관측 원천과 기본 Mock 경계의 Service를 구성합니다. */
    private static AdminOverviewService serviceWithObservationSource(OverviewObservationSource source) {
        return serviceWithSources(new AdminOverviewMockDataFactory(), source);
    }

    /** 테스트가 선택한 Mock 모집단과 관측 원천을 모든 실제 Calculator에 연결합니다. */
    private static AdminOverviewService serviceWithSources(
            AdminOverviewMockDataFactory factory,
            OverviewObservationSource source
    ) {
        TimeProvider timeProvider = new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC));
        OverviewStatusCalculator statusCalculator = new OverviewStatusCalculator();
        return new AdminOverviewService(
                timeProvider,
                factory,
                source,
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

    private static AdminOverviewService service() {
        AdminOverviewMockDataFactory factory = new AdminOverviewMockDataFactory();
        return serviceWithSources(factory, mockObservationSource(factory));
    }

    private static AdminOverviewService serviceWithQueueStatus(Long couponId, SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<QueueInput> inputs = base.queueInputs().stream()
                        .map(input -> input.couponId().equals(couponId)
                                ? queueWithStatus(input, status) : input)
                        .toList();
                return withQueueInputs(base, inputs);
            }
        });
    }

    private static AdminOverviewService serviceWithStockStatus(SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<CampaignOverviewSource> campaigns = base.campaigns().stream()
                        .map(source -> source.couponId().equals(103L)
                                ? new CampaignOverviewSource(source.couponId(), source.campaignName(),
                                source.brandName(), source.status(), source.opensAt(), source.closesAt(),
                                source.engineVersion(), status.carriesValue() ? 10_000L : null,
                                status.carriesValue() ? 3_700L : null,
                                status.carriesValue() ? snapshotAt : null, status,
                                source.preparationCompleted()) : source)
                        .toList();
                return withCampaigns(base, campaigns);
            }
        });
    }

    private static AdminOverviewService serviceWithIssuanceStatus(Long couponId, SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> inputs = base.issuanceFlowInputs().stream()
                        .map(input -> input.couponId().equals(couponId)
                                ? issuanceWithStatus(input, status, snapshotAt) : input)
                        .toList();
                return withIssuanceFlowInputs(base, inputs);
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
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> issuanceInputs = base.issuanceFlowInputs().stream()
                        .map(input -> issuanceSourceInput(input,
                                input.couponId().equals(101L) ? issuanceStatus : SourceStatus.N_A, snapshotAt))
                        .toList();
                List<QueueInput> queueInputs = base.queueInputs().stream()
                        .map(input -> queueSourceInput(input,
                                input.couponId().equals(102L) ? queueStatus : SourceStatus.N_A, snapshotAt))
                        .toList();
                return withActionInputs(base, issuanceInputs, queueInputs);
            }
        });
    }

    /** 모든 O1·O2 원천을 같은 상태로 바꿔 전체 N_A·NO_TRAFFIC 분기를 검증합니다. */
    private static AdminOverviewService serviceWithAllActionSourceStatus(SourceStatus status) {
        return serviceWithDataset(new AdminOverviewMockDataFactory() {
            @Override
            public AdminOverviewMockDataset create(Instant snapshotAt) {
                AdminOverviewMockDataset base = super.create(snapshotAt);
                List<IssuanceFlowInput> issuanceInputs = base.issuanceFlowInputs().stream()
                        .map(input -> issuanceSourceInput(input, status, snapshotAt))
                        .toList();
                List<QueueInput> queueInputs = base.queueInputs().stream()
                        .map(input -> queueSourceInput(input, status, snapshotAt))
                        .toList();
                return withActionInputs(base, issuanceInputs, queueInputs);
            }
        });
    }

    private static AdminOverviewService serviceWithDataset(AdminOverviewMockDataFactory factory) {
        return serviceWithSources(factory, mockObservationSource(factory));
    }

    /** O1 입력만 교체하고 나머지 Dataset 원천은 그대로 보존합니다. */
    private static AdminOverviewMockDataset withIssuanceFlowInputs(
            AdminOverviewMockDataset base,
            List<IssuanceFlowInput> issuanceFlowInputs
    ) {
        return copyDataset(base, issuanceFlowInputs, base.queueInputs(), base.campaigns());
    }

    /** O2 입력만 교체하고 나머지 Dataset 원천은 그대로 보존합니다. */
    private static AdminOverviewMockDataset withQueueInputs(
            AdminOverviewMockDataset base,
            List<QueueInput> queueInputs
    ) {
        return copyDataset(base, base.issuanceFlowInputs(), queueInputs, base.campaigns());
    }

    /** 캠페인 원천만 교체하고 나머지 Dataset 원천은 그대로 보존합니다. */
    private static AdminOverviewMockDataset withCampaigns(
            AdminOverviewMockDataset base,
            List<CampaignOverviewSource> campaigns
    ) {
        return copyDataset(base, base.issuanceFlowInputs(), base.queueInputs(), campaigns);
    }

    /** Action 상태 합성 테스트에서 O1·O2 입력만 함께 교체합니다. */
    private static AdminOverviewMockDataset withActionInputs(
            AdminOverviewMockDataset base,
            List<IssuanceFlowInput> issuanceFlowInputs,
            List<QueueInput> queueInputs
    ) {
        return copyDataset(base, issuanceFlowInputs, queueInputs, base.campaigns());
    }

    /** 이름 있는 테스트 헬퍼에서만 Dataset의 전체 canonical 생성자를 호출합니다. */
    private static AdminOverviewMockDataset copyDataset(
            AdminOverviewMockDataset base,
            List<IssuanceFlowInput> issuanceFlowInputs,
            List<QueueInput> queueInputs,
            List<CampaignOverviewSource> campaigns
    ) {
        return new AdminOverviewMockDataset(base.policy(), issuanceFlowInputs, queueInputs, base.outcomeInput(),
                campaigns, base.preparationActionCandidates(), base.consistencyActionContexts(),
                base.aggregateIssuanceRate(), base.latencySummary());
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
        return stoppedIssuanceInput(input, snapshotAt, snapshotAt);
    }

    /** 평가 종료와 raw scrape 시각을 분리한 STOPPED O1 입력을 만듭니다. */
    private static IssuanceFlowInput stoppedIssuanceInput(
            IssuanceFlowInput input,
            Instant snapshotAt,
            Instant observedAt
    ) {
        return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), input.stockAvailable(),
                input.windowStart(), input.windowEnd(), input.trendWindowStart(), input.trendWindowEnd(),
                input.attemptedCount(), 0d,
                input.comparisonCompletedCount(), input.comparisonWindowStart(), input.comparisonWindowEnd(),
                List.of(), null, snapshotAt.minus(Duration.ofMinutes(10)), SourceStatus.VALID, observedAt);
    }

    /** 값 있는 최신성 상태는 더 오래된 실제 O1 관측 시각을 포함하고 값 없는 상태는 null로 만듭니다. */
    private static IssuanceFlowInput issuanceWithStatus(
            IssuanceFlowInput input,
            SourceStatus status,
            Instant snapshotAt
    ) {
        if (!status.carriesValue()) {
            return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), null, null, null, null, null,
                    null, null, null, null, null, null, null, null, status, null);
        }
        Instant observedAt = snapshotAt.minus(Duration.ofMinutes(5));
        Instant windowStart = observedAt.minus(Duration.ofMinutes(1));
        Instant comparisonWindowStart = windowStart.minus(Duration.ofMinutes(1));
        return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), true, windowStart, observedAt,
                windowStart, observedAt,
                10d, 10d, 10d, comparisonWindowStart, windowStart,
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
            return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), null, null, null, null, null,
                    null, null, null, null, null, null, null, null, status, null);
        }
        Instant observedAt = snapshotAt.minus(Duration.ofMinutes(5));
        Instant windowStart = observedAt.minus(Duration.ofMinutes(1));
        Instant comparisonWindowStart = windowStart.minus(Duration.ofMinutes(1));
        if (status == SourceStatus.NO_TRAFFIC) {
            return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), true, windowStart, observedAt,
                    windowStart, observedAt,
                    0d, 0d, 0d, comparisonWindowStart, windowStart, List.of(), null, windowStart, status, observedAt);
        }
        return new IssuanceFlowInput(input.couponId(), input.campaignStatus(), true, windowStart, observedAt,
                windowStart, observedAt,
                10d, 10d, 10d, comparisonWindowStart, windowStart,
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

    private static <T> AdminOverviewSnapshot.Observation<T> pendingObservation() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null);
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
        public AdminOverviewMockDataset create(Instant snapshotAt) {
            createCount++;
            events.add("factory");
            return super.create(snapshotAt);
        }
    }

    /** 실제 기술 중립 묶음을 반환하면서 Service의 단일 observe 호출을 기록합니다. */
    private static final class RecordingOverviewObservationSource implements OverviewObservationSource {

        private final List<String> events;
        private int observeCount;
        private OverviewObservationData result;

        private RecordingOverviewObservationSource(List<String> events) {
            this.events = events;
        }

        @Override
        public OverviewObservationData observe(OverviewObservationRequest request) {
            observeCount++;
            events.add("observation");
            AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(request.snapshotAt());
            result = mockObservationData(request, dataset);
            return result;
        }
    }

    /** 실제 O1 계산 결과를 유지하는 호출 계수기입니다. */
    private static final class RecordingIssuanceFlowCalculator extends IssuanceFlowCalculator {

        private final List<String> events;
        private int calculateCount;
        private IssuanceFlowCalculation observedResult;
        private IssuanceFlowCalculation mockResult;

        private RecordingIssuanceFlowCalculator(List<String> events) {
            this.events = events;
        }

        @Override
        public IssuanceFlowCalculation calculate(OverviewCalculationPolicy policy, List<IssuanceFlowInput> inputs) {
            calculateCount++;
            events.add("issuance");
            IssuanceFlowCalculation result = super.calculate(policy, inputs);
            if (calculateCount == 1) {
                observedResult = result;
            } else {
                mockResult = result;
            }
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

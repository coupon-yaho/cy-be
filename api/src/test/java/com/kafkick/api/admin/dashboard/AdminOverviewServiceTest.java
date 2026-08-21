package com.kafkick.api.admin.dashboard;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.dashboard.AdminOverviewResult.OverallStatus;
import com.kafkick.api.admin.dashboard.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * 실제 운영 원천이 연결되기 전 관리자 운영현황 Service의 기본 응답 규칙을 검증합니다.
 *
 * <p>미수집 값을 숫자 0이나 빈 목록으로 대신하면 화면은 실제 정상 관측 결과로 해석할 수 있습니다.
 * 따라서 Service는 응답 조립 시각만 제공하고, 원천이 필요한 모든 영역은 명시적인
 * {@link SourceStatus#UNAVAILABLE} 상태로 유지해야 합니다.</p>
 */
class AdminOverviewServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-20T03:15:00Z");

    /**
     * 원천이 없는 상태를 빈 정상값으로 위장하거나 응답 조립 시각을 누락하는 회귀를 방지합니다.
     */
    @Test
    @DisplayName("운영 원천 미연결 시 조립 시각과 모든 영역의 UNAVAILABLE 상태를 반환한다")
    void returnsUnavailableOverviewUntilOperationalSourcesAreConnected() {
        AdminOverviewService service = service();

        AdminOverviewResult result = service.getOverview();
        AdminOverviewSnapshot snapshot = result.snapshot();

        assertThat(snapshot.snapshotAt()).isEqualTo(NOW);
        assertThat(result.overallStatus()).isEqualTo(OverallStatus.UNAVAILABLE);
        assertThat(observations(snapshot))
                .allSatisfy(observation -> {
                    assertThat(observation.value()).isNull();
                    assertThat(observation.status()).isEqualTo(SourceStatus.UNAVAILABLE);
                    assertThat(observation.observedAt()).isNull();
                });
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
        return new AdminOverviewService(timeProvider, statusCalculator);
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
}

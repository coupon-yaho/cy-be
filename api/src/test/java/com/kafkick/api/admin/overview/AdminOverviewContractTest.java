package com.kafkick.api.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.coupon.CouponStatus;

/**
 * 운영 현황 Adapter 경계가 후속 인프라 구현과 분리되는지 검증합니다.
 *
 * <p>이 테스트의 통과는 Provider 인터페이스와 내부 Snapshot 계약이 준비됐다는 뜻이며,
 * 실제 데이터 조회나 {@code GET /api/v1/admin/overview} 기능 구현 완료를 뜻하지 않습니다.</p>
 */
class AdminOverviewContractTest {

    private static final Instant FROM = Instant.parse("2026-08-17T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-08-17T01:00:00Z");

    /** Provider 메서드명·인자·반환 타입이 확정된 대표 시그니처와 정확히 일치하는지 검증합니다. */
    @Test
    @DisplayName("AdminOverviewProvider는 확정된 getOverview 시그니처를 제공한다")
    void providerKeepsDocumentedSignature() throws Exception {
        Method method = AdminOverviewProvider.class.getDeclaredMethod("getOverview", AdminOverviewQuery.class);

        assertThat(method.getReturnType()).isEqualTo(AdminOverviewSnapshot.class);
        assertThat(AdminOverviewProvider.class.getDeclaredMethods()).containsExactly(method);
    }

    /** 문서에서 확정한 세 조회 성분을 HTTP annotation이나 임의 기본값 없이 그대로 보존하는지 검증합니다. */
    @Test
    @DisplayName("AdminOverviewQuery는 기간과 couponId 집합을 그대로 보존한다")
    void queryPreservesConfirmedComponents() {
        Set<Long> couponIds = Set.of(11L, 22L);

        AdminOverviewQuery query = new AdminOverviewQuery(FROM, TO, couponIds);

        assertThat(query.from()).isEqualTo(FROM);
        assertThat(query.to()).isEqualTo(TO);
        assertThat(query.couponIds()).containsExactlyInAnyOrder(11L, 22L);
    }

    /** null과 빈 집합의 의미가 아직 확정되지 않았으므로 생성 단계에서 임의 정규화하지 않는지 검증합니다. */
    @Test
    @DisplayName("AdminOverviewQuery는 미확정 null과 빈 집합 규칙을 강제하지 않는다")
    void queryDoesNotInventUnconfirmedNullOrEmptyRules() {
        AdminOverviewQuery nullable = new AdminOverviewQuery(null, null, null);
        AdminOverviewQuery empty = new AdminOverviewQuery(FROM, TO, Set.of());

        assertThat(nullable.from()).isNull();
        assertThat(nullable.to()).isNull();
        assertThat(nullable.couponIds()).isNull();
        assertThat(empty.couponIds()).isEmpty();
    }

    /** 실제 0과 미관측 null이 동일한 숫자로 축약되는 회귀를 방지합니다. */
    @Test
    @DisplayName("Snapshot 관측값은 실제 0과 미관측 null을 SourceStatus로 구분한다")
    void snapshotDistinguishesObservedZeroFromUnobservedValue() {
        AdminOverviewSnapshot.Observation<Long> observedZero =
                new AdminOverviewSnapshot.Observation<>(0L, SourceStatus.VALID, TO);
        AdminOverviewSnapshot.Observation<Long> unobserved =
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);

        assertThat(observedZero.value()).isZero();
        assertThat(observedZero.status()).isEqualTo(SourceStatus.VALID);
        assertThat(observedZero.observedAt()).isEqualTo(TO);
        assertThat(unobserved.value()).isNull();
        assertThat(unobserved.status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(unobserved.observedAt()).isNull();
    }

    /** Overview 내부 Observation도 canonical 상태·관측 시각 불변식을 따르는지 검증합니다. */
    @Test
    @DisplayName("Snapshot Observation은 상태와 관측 시각 불변식을 위반할 수 없다")
    void snapshotObservationFollowsCanonicalStateAndTimeInvariant() {
        assertThatThrownBy(() -> new AdminOverviewSnapshot.Observation<>(
                1L, SourceStatus.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.Observation<>(
                null, SourceStatus.UNAVAILABLE, TO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.Observation<>(
                1L, SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** DB·Redis·Kafka·Servlet·Spring 타입이 Snapshot 구성요소로 유입되는 회귀를 방지합니다. */
    @Test
    @DisplayName("AdminOverviewSnapshot은 기술 인프라 타입에 의존하지 않는다")
    void snapshotHasNoInfrastructureTypeDependency() {
        List<String> componentTypeNames = Stream.concat(
                        Stream.of(AdminOverviewSnapshot.class),
                        Arrays.stream(AdminOverviewSnapshot.class.getDeclaredClasses()))
                .filter(Class::isRecord)
                .flatMap(type -> Arrays.stream(type.getRecordComponents()))
                .map(RecordComponent::getType)
                .map(Class::getName)
                .toList();

        assertThat(componentTypeNames).noneMatch(typeName ->
                typeName.startsWith("org.springframework.")
                        || typeName.startsWith("jakarta.servlet.")
                        || typeName.startsWith("com.kafkick.storage.")
                        || typeName.startsWith("com.kafkick.infra.")
                        || typeName.startsWith("com.kafkick.api.admin.issuance.")
                        || typeName.startsWith("com.kafkick.api.admin.notification.")
                        || typeName.startsWith("com.kafkick.api.admin.observability.dto."));
    }

    /** Snapshot의 시간과 권장 행동이 기술 단위나 프론트 전용 문구로 손실되지 않는지 검증합니다. */
    @Test
    @DisplayName("Snapshot은 Duration과 서버 권장 행동의 코드 문구 목적지를 함께 보존한다")
    void snapshotKeepsDurationAndServerRecommendedAction() {
        AdminOverviewSnapshot.RecommendedAction action = new AdminOverviewSnapshot.RecommendedAction(
                AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                "D2에서 입장 처리 상태 확인",
                AdminOverviewSnapshot.TargetScreen.METRICS);
        AdminOverviewSnapshot.OperationActionItem item = new AdminOverviewSnapshot.OperationActionItem(
                17L,
                "딜리버리고 여름특가",
                FROM,
                Severity.CRITICAL,
                "입장 처리가 멈춰 고객 대기가 지속됩니다.",
                TO,
                Duration.ofSeconds(138),
                action);

        assertThat(item.couponId()).isEqualTo(17L);
        assertThat(item.duration()).isEqualTo(Duration.ofMinutes(2).plusSeconds(18));
        assertThat(item.recommendedAction().code()).isEqualTo(AdminOverviewSnapshot.ActionCode.QUEUE_STALLED);
        assertThat(item.recommendedAction().displayText()).isEqualTo("D2에서 입장 처리 상태 확인");
        assertThat(item.recommendedAction().targetScreen()).isEqualTo(AdminOverviewSnapshot.TargetScreen.METRICS);
    }

    /** `/overview`가 소유한 전체 집계 4종과 O1~O4 캠페인 계약을 한 Snapshot으로 표현하는지 검증합니다. */
    @Test
    @DisplayName("Snapshot은 전체 집계 4종과 campaigns의 O1 O2 O4 및 최상위 O3를 표현한다")
    void snapshotRepresentsAllOverviewOwnedSections() {
        AdminOverviewSnapshot.CampaignOverview campaign = new AdminOverviewSnapshot.CampaignOverview(
                1, 17L, "딜리버리고 여름특가", "딜리버리고", CouponStatus.OPEN, FROM, TO,
                Severity.CRITICAL,
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.IssuanceFlow(
                                44.0,
                                FROM,
                                TO,
                                List.of(new AdminOverviewSnapshot.IssuanceRatePoint(TO, 44.0)),
                                AdminOverviewSnapshot.IssuanceFlowState.DECREASING,
                                Duration.ofMinutes(2)),
                        SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.CampaignQueueStatus(
                                3204, AdminOverviewSnapshot.TrendDirection.INCREASING, 180,
                                0.0, null, AdminOverviewSnapshot.CampaignQueueAssessment.ADMISSION_STOPPED),
                        SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.StockForecast(4650, 15000, 0.31, null),
                        SourceStatus.VALID, TO),
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "신규 고객 대기 지속",
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "D2에서 입장 처리 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS));
        AdminOverviewSnapshot.CustomerOutcomeSummary outcomes = new AdminOverviewSnapshot.CustomerOutcomeSummary(
                FROM, TO, 12558,
                List.of(new AdminOverviewSnapshot.CustomerOutcome(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED,
                        1847, 0.147, "쿠폰이 정상 발급됨")));

        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                TO,
                unavailable(), unavailable(), unavailable(), unavailable(),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.AggregateIssuanceRate(612.0, 840.0), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.AggregateQueue(3388, 35.7, Duration.ofSeconds(95)),
                        SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(
                                Duration.ofMillis(84), Duration.ofMillis(110), FROM, TO),
                        SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.CampaignStatusSummary(3, 1, 12), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(List.of(), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(List.of(campaign), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(outcomes, SourceStatus.VALID, TO));

        assertThat(snapshot.aggregateIssuanceRate().value().currentPerSecond()).isEqualTo(612.0);
        assertThat(snapshot.aggregateQueue().value().waitingCount()).isEqualTo(3388);
        assertThat(snapshot.latencySummary().value().successfulP99()).isEqualTo(Duration.ofMillis(84));
        assertThat(snapshot.campaignStatusSummary().value().openCount()).isEqualTo(3);
        assertThat(snapshot.campaigns().value()).containsExactly(campaign);
        assertThat(snapshot.customerOutcomes().value().outcomes().getFirst().ratio()).isEqualTo(0.147);
        assertThat(Arrays.stream(AdminOverviewSnapshot.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("issuanceInquiries", "issuanceHistories", "notificationSummary", "events");
    }

    /** HTML의 O3 결과 구분이 축약되거나 임의 코드가 추가되는 회귀를 방지합니다. */
    @Test
    @DisplayName("O3 고객 결과 코드는 확정된 7종만 제공한다")
    void customerOutcomeTypeKeepsExactlySevenConfirmedValues() {
        assertThat(Arrays.stream(AdminOverviewSnapshot.CustomerOutcomeType.values())
                .map(Enum::name))
                .containsExactly(
                        "ISSUED",
                        "QUEUED",
                        "ALREADY_ISSUED",
                        "STOCK_EXHAUSTED",
                        "INELIGIBLE",
                        "ENTRY_EXPIRED",
                        "SYSTEM_FAILURE");
    }

    /** O3 비율의 0~1 계약이 잘못된 Adapter 값이나 NaN을 HTTP 계층까지 전달하지 않도록 검증합니다. */
    @Test
    @DisplayName("O3 고객 결과 비율은 유한한 0 이상 1 이하 값만 허용한다")
    void customerOutcomeRatioAcceptsOnlyFiniteUnitInterval() {
        assertThat(new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0, 0.0, "발급 없음").ratio())
                .isZero();
        assertThat(new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, 1.0, "모두 발급").ratio())
                .isOne();

        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, -0.001, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, 1.001, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewSnapshot.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, Double.NaN, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 조치 목록이 없을 때 null 대신 명시적인 빈 목록을 전달할 수 있는 계약인지 검증합니다. */
    @Test
    @DisplayName("Snapshot 조치 목록은 빈 목록을 값으로 표현할 수 있다")
    void snapshotSupportsObservedEmptyActionList() {
        AdminOverviewSnapshot snapshot = snapshotWithActions(List.of());

        assertThat(snapshot.actionItems().value()).isEmpty();
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.VALID);
    }

    private AdminOverviewSnapshot snapshotWithActions(List<AdminOverviewSnapshot.OperationActionItem> actions) {
        return new AdminOverviewSnapshot(
                TO,
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.ActionRequiredSummary(0, 0, 0), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.OpeningSoonSummary(0, 0), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.QueueRiskSummary(0, null), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.StockRiskSummary(0, null), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.AggregateIssuanceRate(0, 0), SourceStatus.NO_TRAFFIC, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.AggregateQueue(0, 0, Duration.ZERO),
                        SourceStatus.NO_TRAFFIC, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(null, null, FROM, TO),
                        SourceStatus.NO_TRAFFIC, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.CampaignStatusSummary(0, 0, 0), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(actions, SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(List.of(), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.CustomerOutcomeSummary(FROM, TO, 0, List.of()),
                        SourceStatus.NO_TRAFFIC, TO));
    }

    private <T> AdminOverviewSnapshot.Observation<T> unavailable() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }
}

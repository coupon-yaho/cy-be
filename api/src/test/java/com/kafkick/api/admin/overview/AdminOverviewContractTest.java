package com.kafkick.api.admin.overview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.dashboard.dto.AdminOverviewResponse;
import com.kafkick.core.admin.couponroundsource.PreparationItem;
import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.coupon.domain.CouponRoundStatus;

/**
 * 운영현황 Snapshot 계약이 후속 인프라 구현과 분리되는지 검증합니다.
 *
 * <p>이 테스트는 기술 중립 Snapshot과 상태 불변식을 검증하며 실제 Repository나 관측 원천 연결
 * 완료를 의미하지 않습니다.</p>
 */
class AdminOverviewContractTest {

    private static final Instant FROM = Instant.parse("2026-08-17T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-08-17T01:00:00Z");

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
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "입장 처리가 멈춰 고객 대기가 지속됩니다.",
                TO,
                Duration.ofSeconds(138),
                action);

        assertThat(item.couponId()).isEqualTo(17L);
        assertThat(item.customerImpact()).isEqualTo(AdminOverviewSnapshot.CustomerImpact.WIDESPREAD);
        assertThat(item.duration()).isEqualTo(Duration.ofMinutes(2).plusSeconds(18));
        assertThat(item.recommendedAction().code()).isEqualTo(AdminOverviewSnapshot.ActionCode.QUEUE_STALLED);
        assertThat(item.recommendedAction().displayText()).isEqualTo("D2에서 입장 처리 상태 확인");
        assertThat(item.recommendedAction().targetScreen()).isEqualTo(AdminOverviewSnapshot.TargetScreen.METRICS);
    }

    /** `/overview`가 소유한 전체 집계 4종과 O1~O4 쿠폰 회차 계약을 한 Snapshot으로 표현하는지 검증합니다. */
    @Test
    @DisplayName("Snapshot은 전체 집계 4종과 couponRounds의 O1 O2 O4 및 최상위 O3를 표현한다")
    void snapshotRepresentsAllOverviewOwnedSections() {
        AdminOverviewSnapshot.CouponRoundOverview couponRound = new AdminOverviewSnapshot.CouponRoundOverview(
                1, 17L, "딜리버리고 여름특가", "딜리버리고", CouponRoundStatus.OPEN, FROM, TO,
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
                        new AdminOverviewSnapshot.CouponRoundQueueStatus(
                                3204, AdminOverviewSnapshot.TrendDirection.INCREASING, 180,
                                0.0, null, AdminOverviewSnapshot.CouponRoundQueueAssessment.ADMISSION_STOPPED),
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
                List.of(
                        new AdminOverviewSnapshot.CustomerOutcome(
                                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED,
                                1847, 1847d / 12558d, "쿠폰이 정상 발급됨"),
                        new AdminOverviewSnapshot.CustomerOutcome(
                                AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE,
                                10711, 10711d / 12558d, "시스템 실패")));

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
                        new AdminOverviewSnapshot.CouponRoundStatusSummary(3, 1, 12), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.ActionItemSnapshot(0, List.of()),
                        SourceStatus.VALID,
                        TO),
                new AdminOverviewSnapshot.Observation<>(List.of(couponRound), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(outcomes, SourceStatus.VALID, TO));

        assertThat(snapshot.aggregateIssuanceRate().value().currentPerSecond()).isEqualTo(612.0);
        assertThat(snapshot.aggregateQueue().value().waitingCount()).isEqualTo(3388);
        assertThat(snapshot.latencySummary().value().successfulP99()).isEqualTo(Duration.ofMillis(84));
        assertThat(snapshot.couponRoundStatusSummary().value().openCount()).isEqualTo(3);
        assertThat(snapshot.couponRounds().value()).containsExactly(couponRound);
        assertThat(snapshot.customerOutcomes().value().outcomes().getFirst().ratio())
                .isEqualTo(1847d / 12558d);
        assertThat(Arrays.stream(AdminOverviewSnapshot.class.getRecordComponents())
                .map(RecordComponent::getName))
                .doesNotContain("issuanceInquiries", "issuanceHistories", "notificationSummary", "events");
    }

    /**
     * HTML의 O3 결과 구분이 축약되거나 임의 코드가 추가되는 회귀를 방지합니다.
     *
     * <p>{@code RETRY_IN_PROGRESS} 는 v2 가 만든 새 고객 결과다 — v1 은 이 자리에서 폴링하며
     * 기다려 클라이언트에게 이 상태가 보이지 않았다. 기존 7종의 이름과 순서는 그대로 두고
     * 끝에 붙였다: 선언 순서가 화면 행 순서라 중간에 넣으면 기존 행이 밀린다.
     */
    @Test
    @DisplayName("O3 고객 결과 코드는 확정된 8종만 제공한다")
    void customerOutcomeTypeKeepsExactlyEightConfirmedValues() {
        assertThat(Arrays.stream(AdminOverviewSnapshot.CustomerOutcomeType.values())
                .map(Enum::name))
                .containsExactly(
                        "ISSUED",
                        "QUEUED",
                        "ALREADY_ISSUED",
                        "STOCK_EXHAUSTED",
                        "INELIGIBLE",
                        "ENTRY_EXPIRED",
                        "SYSTEM_FAILURE",
                        "RETRY_IN_PROGRESS");
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

        assertThat(snapshot.actionItems().value().totalCount()).isZero();
        assertThat(snapshot.actionItems().value().topItems()).isEmpty();
        assertThat(snapshot.actionItems().status()).isEqualTo(SourceStatus.VALID);
    }

    /** 준비 미완료 판정이 DTO 필드 추가 없이 기존 네 HTTP 영역에 함께 노출되는지 검증합니다. */
    @Test
    @DisplayName("준비 미완료는 기존 HTTP DTO의 KPI 조치 목록 쿠폰 회차 행에 함께 노출된다")
    void exposesIncompletePreparationAcrossExistingHttpResponseShape() {
        AdminOverviewSnapshot.RecommendedAction recommendedAction =
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.COUPON_ROUND_NOT_READY,
                        "쿠폰 회차 준비 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.COUPON_ROUND_DETAIL);
        AdminOverviewSnapshot.OperationActionItem action =
                new AdminOverviewSnapshot.OperationActionItem(
                        17L, "딜리버리고 여름특가", TO, Severity.WARN,
                        AdminOverviewSnapshot.CustomerImpact.NONE,
                        "오픈 전 필수 준비 항목을 확인해야 합니다.",
                        TO.minus(Duration.ofMinutes(30)), null, recommendedAction);
        AdminOverviewSnapshot.CouponRoundOverview couponRound = new AdminOverviewSnapshot.CouponRoundOverview(
                1, 17L, "딜리버리고 여름특가", "딜리버리고", CouponRoundStatus.SCHEDULED,
                TO, TO.plus(Duration.ofHours(1)), Severity.WARN,
                unavailable(), unavailable(), unavailable(),
                List.of(PreparationItem.REDIS_WARMUP, PreparationItem.REDIS_GATE),
                AdminOverviewSnapshot.CustomerImpact.NONE,
                "오픈 전 필수 준비 항목을 확인해야 합니다.", recommendedAction);
        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                TO,
                observed(new AdminOverviewSnapshot.ActionRequiredSummary(1, 0, 1)),
                observed(new AdminOverviewSnapshot.OpeningSoonSummary(1, 1)),
                unavailable(), unavailable(), unavailable(), unavailable(), unavailable(),
                observed(new AdminOverviewSnapshot.CouponRoundStatusSummary(0, 1, 0)),
                observed(new AdminOverviewSnapshot.ActionItemSnapshot(1, List.of(action))),
                observed(List.of(couponRound)), unavailable());

        AdminOverviewResponse response = AdminOverviewResponse.from(snapshot, OverallStatus.PARTIAL);

        assertThat(Arrays.stream(AdminOverviewResponse.class.getRecordComponents())
                .map(RecordComponent::getName))
                .containsExactly(
                        "snapshotAt", "overallStatus", "actionRequired", "openingSoon", "queueRisk",
                        "stockRisk", "aggregateIssuanceRate", "aggregateQueue", "latencySummary",
                        "couponRoundStatusSummary", "actionItems", "couponRounds", "customerOutcomes");
        assertThat(response.openingSoon().value().preparationIncompleteCount()).isEqualTo(1L);
        assertThat(response.actionRequired().value().warningCount()).isEqualTo(1L);
        AdminOverviewResponse.OperationActionItem responseAction =
                response.actionItems().value().topItems().getFirst();
        assertThat(responseAction.recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.COUPON_ROUND_NOT_READY);
        AdminOverviewResponse.CouponRoundOverview responseCouponRound = response.couponRounds().value().getFirst();
        assertThat(responseCouponRound.severity()).isEqualTo(Severity.WARN);
        assertThat(responseCouponRound.failedPreparationItems())
                .containsExactly(PreparationItem.REDIS_WARMUP, PreparationItem.REDIS_GATE);
        assertThat(responseCouponRound.customerImpact()).isEqualTo(responseAction.customerImpact());
        assertThat(responseCouponRound.recommendedAction()).isEqualTo(responseAction.recommendedAction());
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
                        new AdminOverviewSnapshot.CouponRoundStatusSummary(0, 0, 0), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.ActionItemSnapshot(actions.size(), actions),
                        SourceStatus.VALID,
                        TO),
                new AdminOverviewSnapshot.Observation<>(List.of(), SourceStatus.VALID, TO),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.CustomerOutcomeSummary(FROM, TO, 0, List.of()),
                        SourceStatus.NO_TRAFFIC, TO));
    }

    private <T> AdminOverviewSnapshot.Observation<T> unavailable() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }

    private <T> AdminOverviewSnapshot.Observation<T> observed(T value) {
        return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, TO);
    }
}

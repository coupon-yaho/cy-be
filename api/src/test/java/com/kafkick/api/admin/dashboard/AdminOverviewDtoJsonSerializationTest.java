package com.kafkick.api.admin.dashboard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.api.admin.dashboard.dto.AdminOverviewResponse;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.api.admin.support.AdminJsonTest;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.campaignsource.PreparationItem;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.coupon.domain.CouponRoundStatus;

/**
 * 운영 현황 응답 초안의 JSON 필드, enum 이름, null과 빈 목록 표현을 고정합니다.
 *
 * <p>이 테스트는 HTTP 응답 계약을 검증할 뿐이며, 현재 Overview API가 실제 데이터를
 * 조회하거나 200을 반환한다는 의미가 아닙니다.</p>
 */
@AdminJsonTest
class AdminOverviewDtoJsonSerializationTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-17T05:03:58Z");
    private static final Instant OBSERVED_AT = Instant.parse("2026-08-17T05:03:57Z");
    private static final Instant OPENING_OBSERVED_AT = Instant.parse("2026-08-17T05:03:56Z");

    private final ObjectMapper objectMapper;

    @Autowired
    AdminOverviewDtoJsonSerializationTest(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** KPI 누락 필드와 서버 권장 행동 구조가 JSON에서 손실되는 회귀를 방지합니다. */
    @Test
    @DisplayName("Overview JSON은 Duration과 권장 행동 코드 문구 목적지를 함께 직렬화한다")
    void overviewSerializesDurationsAndServerRecommendedAction() throws Exception {
        AdminOverviewResponse response = new AdminOverviewResponse(
                SNAPSHOT_AT,
                OverallStatus.PARTIAL,
                new ObservedValue<>(
                        new AdminOverviewResponse.ActionRequiredSummary(2, 1, 1),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.OpeningSoonSummary(2, 1),
                        SourceStatus.STALE,
                        OPENING_OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.QueueRiskSummary(1, Duration.ofMinutes(12)),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null),
                unavailable(),
                unavailable(),
                unavailable(),
                unavailable(),
                new ObservedValue<>(
                        new AdminOverviewResponse.ActionItemSummary(
                                1,
                                List.of(new AdminOverviewResponse.OperationActionItem(
                                        17L,
                                        "딜리버리고 여름특가",
                                        Instant.parse("2026-08-17T04:40:00Z"),
                                        Severity.CRITICAL,
                                        AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                                        "입장 처리가 멈춰 고객 대기가 지속됩니다.",
                                        Instant.parse("2026-08-17T05:01:40Z"),
                                        Duration.ofSeconds(138),
                                        new AdminOverviewResponse.RecommendedAction(
                                                AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                                                "D2에서 입장 처리 상태 확인",
                                                AdminOverviewSnapshot.TargetScreen.METRICS)))),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                unavailable(),
                unavailable());

        String json = objectMapper.writeValueAsString(response);

        assertThat(json)
                .contains("\"snapshotAt\":\"2026-08-17T05:03:58Z\"")
                .contains("\"actionRequired\":{\"value\":{\"totalCount\":2,\"urgentCount\":1,\"warningCount\":1},\"state\":\"VALID\",\"observedAt\":\"2026-08-17T05:03:57Z\"}")
                .contains("\"openingSoon\":{\"value\":{\"totalCount\":2,\"preparationIncompleteCount\":1},\"state\":\"STALE\",\"observedAt\":\"2026-08-17T05:03:56Z\"}")
                .contains("\"longestWait\":\"PT12M\"")
                .contains("\"campaignName\":\"딜리버리고 여름특가\"")
                .contains("\"duration\":\"PT2M18S\"")
                .contains("\"code\":\"QUEUE_STALLED\"")
                .contains("\"displayText\":\"D2에서 입장 처리 상태 확인\"")
                .contains("\"targetScreen\":\"METRICS\"")
                .contains("\"stockRisk\":{\"state\":\"UNAVAILABLE\"}");
    }

    /** 관측된 빈 조치 목록과 미관측 nullable 시간값이 서로 다른 JSON 상태를 유지하는지 검증합니다. */
    @Test
    @DisplayName("Overview JSON은 빈 조치 목록과 nullable 시간을 보존한다")
    void overviewPreservesEmptyActionsAndNullableDurations() throws Exception {
        AdminOverviewResponse response = new AdminOverviewResponse(
                SNAPSHOT_AT,
                OverallStatus.COMPLETE,
                new ObservedValue<>(
                        new AdminOverviewResponse.ActionRequiredSummary(0, 0, 0),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.OpeningSoonSummary(0, 0),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.QueueRiskSummary(0, null),
                        SourceStatus.NO_TRAFFIC,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.StockRiskSummary(0, null),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.AggregateIssuanceRate(0, 0),
                        SourceStatus.NO_TRAFFIC,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.AggregateQueue(0, 0, Duration.ZERO),
                        SourceStatus.NO_TRAFFIC,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.LatencySummary(null, null, SNAPSHOT_AT, SNAPSHOT_AT),
                        SourceStatus.NO_TRAFFIC,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.CampaignStatusSummary(0, 0, 0),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.ActionItemSummary(0, List.of()),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                new ObservedValue<>(List.of(), SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.CustomerOutcomeSummary(
                                SNAPSHOT_AT.minusSeconds(300), SNAPSHOT_AT, 0, List.of()),
                        SourceStatus.NO_TRAFFIC,
                        OBSERVED_AT));

        assertThat(objectMapper.writeValueAsString(response))
                .contains("\"topItems\":[]")
                .contains("\"campaigns\":{\"value\":[],\"state\":\"VALID\"")
                .contains("\"state\":\"NO_TRAFFIC\"")
                .doesNotContain("\"longestWait\":", "\"nearestDepletion\":");
    }

    /** 정합성 실패 권장 행동의 공개 enum 코드가 Overview JSON에서 바뀌지 않아야 합니다. */
    @Test
    @DisplayName("Overview JSON은 CONSISTENCY_FAILURE 권장 행동 코드를 그대로 직렬화한다")
    void overviewSerializesConsistencyFailureActionCode() throws Exception {
        AdminOverviewResponse response = new AdminOverviewResponse(
                SNAPSHOT_AT,
                OverallStatus.PARTIAL,
                unavailable(), unavailable(), unavailable(), unavailable(),
                unavailable(), unavailable(), unavailable(), unavailable(),
                new ObservedValue<>(
                        new AdminOverviewResponse.ActionItemSummary(
                                1,
                                List.of(new AdminOverviewResponse.OperationActionItem(
                                        17L,
                                        "정합성 확인 쿠폰",
                                        null,
                                        Severity.CRITICAL,
                                        AdminOverviewSnapshot.CustomerImpact.LIMITED,
                                        "정합성 불일치를 확인해야 합니다.",
                                        OBSERVED_AT,
                                        null,
                                        new AdminOverviewResponse.RecommendedAction(
                                                AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE,
                                                "정합성 확인",
                                                AdminOverviewSnapshot.TargetScreen.METRICS)))),
                        SourceStatus.VALID,
                        OBSERVED_AT),
                unavailable(),
                unavailable());

        assertThat(objectMapper.writeValueAsString(response))
                .contains("\"code\":\"CONSISTENCY_FAILURE\"");
    }

    /** 집계 4종, campaigns의 O1·O2·O4, 최상위 O3가 완전한 HTTP JSON 계약으로 직렬화되는지 검증합니다. */
    @Test
    @DisplayName("Overview JSON은 전체 집계와 campaigns O1 O2 O4 및 O3 결과를 직렬화한다")
    void overviewSerializesAggregatesCampaignsAndCustomerOutcomes() throws Exception {
        AdminOverviewResponse.CampaignOverview campaign = new AdminOverviewResponse.CampaignOverview(
                1, 17L, "딜리버리고 여름특가", "딜리버리고", CouponRoundStatus.OPEN,
                Instant.parse("2026-08-17T04:40:00Z"), null, Severity.CRITICAL,
                new ObservedValue<>(
                        new AdminOverviewResponse.IssuanceFlow(
                                44.0,
                                Instant.parse("2026-08-17T04:53:58Z"),
                                SNAPSHOT_AT,
                                List.of(new AdminOverviewResponse.IssuanceRatePoint(OBSERVED_AT, 44.0)),
                                AdminOverviewSnapshot.IssuanceFlowState.DECREASING,
                                Duration.ofMinutes(2)),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.CampaignQueueStatus(
                                3204, AdminOverviewSnapshot.TrendDirection.INCREASING, 180,
                                0.0, null, AdminOverviewSnapshot.CampaignQueueAssessment.ADMISSION_STOPPED),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.StockForecast(4650, 15000, 0.31, null),
                        SourceStatus.VALID, OBSERVED_AT),
                List.of(PreparationItem.ISSUANCE_PATH),
                AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "신규 고객 대기 지속",
                new AdminOverviewResponse.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "D2에서 입장 처리 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.METRICS));
        AdminOverviewResponse response = new AdminOverviewResponse(
                SNAPSHOT_AT,
                OverallStatus.PARTIAL,
                unavailable(), unavailable(), unavailable(), unavailable(),
                new ObservedValue<>(
                        new AdminOverviewResponse.AggregateIssuanceRate(612.0, 840.0),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.AggregateQueue(3388, 35.7, Duration.ofSeconds(95)),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.LatencySummary(
                                Duration.ofMillis(84), Duration.ofMillis(110),
                                Instant.parse("2026-08-17T05:03:48Z"), SNAPSHOT_AT),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.CampaignStatusSummary(3, 1, 12),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(new AdminOverviewResponse.ActionItemSummary(0, List.of()),
                        SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(List.of(campaign), SourceStatus.VALID, OBSERVED_AT),
                new ObservedValue<>(
                        new AdminOverviewResponse.CustomerOutcomeSummary(
                                Instant.parse("2026-08-17T04:53:58Z"), SNAPSHOT_AT, 0.3d,
                                List.of(new AdminOverviewResponse.CustomerOutcome(
                                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED,
                                        0.1d, 1d / 3d, "쿠폰이 정상 발급됨"),
                                        new AdminOverviewResponse.CustomerOutcome(
                                                AdminOverviewSnapshot.CustomerOutcomeType.QUEUED,
                                                0.2d, 2d / 3d, "발급 대기"))),
                        SourceStatus.VALID, OBSERVED_AT));

        String json = objectMapper.writeValueAsString(response);

        assertThat(json)
                .contains("\"aggregateIssuanceRate\":{\"value\":{\"currentPerSecond\":612.0")
                .contains("\"aggregateQueue\":{\"value\":{\"waitingCount\":3388")
                .contains("\"successfulP99\":\"PT0.084S\"")
                .contains("\"campaignStatusSummary\":{\"value\":{\"openCount\":3")
                .contains("\"campaigns\":{\"value\":[{\"priority\":1,\"couponId\":17")
                .contains("\"windowStart\":\"2026-08-17T04:53:58Z\"")
                .contains("\"campaignQueueStatus\":{\"value\":{\"waitingCount\":3204")
                .contains("\"remainingRatio\":0.31")
                .contains("\"failedPreparationItems\":[\"ISSUANCE_PATH\"]")
                .contains("\"customerOutcomes\":{\"value\":{\"windowStart\":")
                .contains("\"totalCount\":0.3")
                .contains("\"type\":\"ISSUED\",\"count\":0.1,\"ratio\":0.3333333333333333")
                .contains("\"campaigns\":{\"value\":[", "\"topItems\":[]")
                .doesNotContain("\"admissionsPerMinute\":0.0,\"estimatedWait\":");
    }

    /** HTTP DTO도 Snapshot과 같은 O3 비율 범위를 유지하여 잘못된 JSON 생성을 차단하는지 검증합니다. */
    @Test
    @DisplayName("Overview HTTP DTO의 O3 비율은 유한한 0 이상 1 이하 값만 허용한다")
    void customerOutcomeResponseRatioAcceptsOnlyFiniteUnitInterval() {
        assertThat(new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0, 0.0, "발급 없음").ratio())
                .isZero();
        assertThat(new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, 1.0, "모두 발급").ratio())
                .isOne();

        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, Double.POSITIVE_INFINITY, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, -0.1, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, 1.1, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, Double.NaN, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1, Double.NEGATIVE_INFINITY, "잘못된 비율"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Prometheus 추정 count는 fractional을 보존하되 비유한·음수는 HTTP 계약으로 나가지 않습니다. */
    @Test
    @DisplayName("Overview HTTP DTO의 O3 count와 total은 유한한 비음수 double이다")
    void customerOutcomeResponseCountsAreFiniteNonNegativeDoubles() {
        AdminOverviewResponse.CustomerOutcome fractional = new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d, 1d, "추정 발급");
        AdminOverviewResponse.CustomerOutcomeSummary summary =
                new AdminOverviewResponse.CustomerOutcomeSummary(
                        SNAPSHOT_AT.minusSeconds(300), SNAPSHOT_AT, 0.1d, List.of(fractional));

        assertThat(fractional.count()).isEqualTo(0.1d);
        assertThat(summary.totalCount()).isEqualTo(0.1d);
        for (double invalid : List.of(-0.1d, Double.NaN, Double.POSITIVE_INFINITY)) {
            assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcome(
                    AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, invalid, 0d, "잘못된 count"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                    SNAPSHOT_AT.minusSeconds(300), SNAPSHOT_AT, invalid, List.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /** DTO가 호출자의 mutable list를 그대로 노출하지 않는지 검증합니다. */
    @Test
    @DisplayName("Overview HTTP O3 outcomes 목록은 생성 후 불변이다")
    void customerOutcomeResponseDefensivelyCopiesOutcomes() {
        List<AdminOverviewResponse.CustomerOutcome> mutable = new java.util.ArrayList<>();
        AdminOverviewResponse.CustomerOutcomeSummary summary =
                new AdminOverviewResponse.CustomerOutcomeSummary(
                        SNAPSHOT_AT.minusSeconds(300), SNAPSHOT_AT, 0d, mutable);

        mutable.add(new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0d, 0d, "발급"));

        assertThat(summary.outcomes()).isEmpty();
        assertThatThrownBy(() -> summary.outcomes().add(new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0d, 0d, "발급")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 직접 생성 가능한 HTTP DTO도 불가능한 O3 요약을 JSON으로 내보내지 않습니다. */
    @Test
    @DisplayName("Overview HTTP O3 summary는 window count ratio 교차 불변식을 소유한다")
    void customerOutcomeResponseRejectsCrossFieldContradictions() {
        AdminOverviewResponse.CustomerOutcome issued = new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d, 1d / 3d, "발급");
        AdminOverviewResponse.CustomerOutcome queued = new AdminOverviewResponse.CustomerOutcome(
                AdminOverviewSnapshot.CustomerOutcomeType.QUEUED, 0.2d, 2d / 3d, "대기");
        Instant from = SNAPSHOT_AT.minusSeconds(300);

        assertThat(new AdminOverviewResponse.CustomerOutcomeSummary(
                from, SNAPSHOT_AT, 0.3d, List.of(issued, queued)).totalCount()).isEqualTo(0.3d);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                null, SNAPSHOT_AT, 0.3d, List.of(issued, queued)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                SNAPSHOT_AT, from, 0.3d, List.of(issued, queued)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                from, SNAPSHOT_AT, 0.2d, List.of(issued, issued)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                from, SNAPSHOT_AT, 0.4d, List.of(issued, queued)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                from, SNAPSHOT_AT, 0d, List.of(new AdminOverviewResponse.CustomerOutcome(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0d, 0d, "발급 없음"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                from, SNAPSHOT_AT, 0.1d, List.of(new AdminOverviewResponse.CustomerOutcome(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0.1d, 0.5d, "잘못된 비율"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 직접 생성 DTO도 가장 작은 양수 total에 빈 outcomes를 허용하지 않습니다. */
    @Test
    @DisplayName("Overview HTTP O3 summary는 양수 total에 outcome을 요구한다")
    void customerOutcomeResponseRejectsEmptyPositiveTotal() {
        Instant from = SNAPSHOT_AT.minusSeconds(300);

        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                from, SNAPSHOT_AT, Double.MIN_VALUE, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 직접 생성 DTO도 큰 total에서 실제 count 합계 불일치를 거부합니다. */
    @Test
    @DisplayName("Overview HTTP O3 summary는 큰 total에서도 정밀한 count 합계를 강제한다")
    void customerOutcomeResponseRejectsLargeTotalMismatch() {
        Instant from = SNAPSHOT_AT.minusSeconds(300);
        double total = 1_000_000_000_000_000d;
        double count = total - 500d;
        assertThatThrownBy(() -> new AdminOverviewResponse.CustomerOutcomeSummary(
                from, SNAPSHOT_AT, total, List.of(new AdminOverviewResponse.CustomerOutcome(
                        AdminOverviewSnapshot.CustomerOutcomeType.ISSUED,
                        count,
                        count / total,
                        "발급"))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** O3 결과 enum이 HTTP JSON에서 확정된 8개 이름으로 그대로 노출되는지 검증합니다. */
    @Test
    @DisplayName("O3 고객 결과 8종은 확정된 enum 이름으로 직렬화된다")
    void customerOutcomeTypeSerializesExactlyEightConfirmedNames() {
        assertThat(Arrays.stream(AdminOverviewSnapshot.CustomerOutcomeType.values())
                .map(objectMapper::writeValueAsString)
                .toList())
                .containsExactly(
                        "\"ISSUED\"",
                        "\"QUEUED\"",
                        "\"ALREADY_ISSUED\"",
                        "\"STOCK_EXHAUSTED\"",
                        "\"INELIGIBLE\"",
                        "\"ENTRY_EXPIRED\"",
                        "\"SYSTEM_FAILURE\"",
                        "\"RETRY_IN_PROGRESS\"");
    }

    /** 공동 기준선에서 확정한 SourceStatus 7종이 모두 enum 이름 그대로 직렬화되는지 검증합니다. */
    @Test
    @DisplayName("SourceStatus 7종은 공동 기준선의 이름으로 직렬화된다")
    void sourceStatusSerializesAllSevenSharedContractNames() {
        assertThat(Arrays.stream(SourceStatus.values())
                .map(objectMapper::writeValueAsString)
                .toList())
                .containsExactly(
                        "\"VALID\"",
                        "\"PENDING\"",
                        "\"WARMING_UP\"",
                        "\"STALE\"",
                        "\"NO_TRAFFIC\"",
                        "\"UNAVAILABLE\"",
                        "\"N_A\"");
    }

    private <T> ObservedValue<T> unavailable() {
        return new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null);
    }
}

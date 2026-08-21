package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.api.admin.observability.dto.AdminMetricsResponse;
import com.kafkick.api.admin.observability.dto.MetricsQuery;
import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.observation.DomainMeterNames;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.observation.SourceStatusCode;
import com.kafkick.core.support.TimeProvider;

/** Prometheus 표본이 값·상태·관측 시각으로 조립되는 규칙을 검증합니다. */
class PromMetricsAssemblerTest {

    private static final Instant NOW = Instant.parse("2026-08-21T00:00:00Z");
    /** instant query 가 표본에 붙여 주는 시각. <b>질의 평가 시각이지 관측 시각이 아니다.</b> */
    private static final Instant EVALUATED = NOW;
    private static final TimeProvider FIXED_TIME = new TimeProvider(Clock.fixed(NOW, ZoneOffset.UTC));
    private static final Duration STALE_AFTER = Duration.ofSeconds(120);

    private static final long FRESH_AGE_SECONDS = 3;
    private static final long STALE_AGE_SECONDS = 300;

    // ── 값이 없을 때 ────────────────────────────────────────────────────────────

    /** 표본이 없는 구간은 0 이 아니라 PENDING 입니다. 0 은 "정상인데 0" 과 구분되지 않습니다. */
    @Test
    @DisplayName("표본이 하나도 없으면 모든 값이 PENDING 이고 0을 만들지 않는다")
    void emptySamplesYieldPending() {
        AdminMetricsResponse response = assemble(FakePromQuery.empty(), globalQuery());

        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.traffic().issueAttemptRps().value()).isNull();
        assertThat(response.latency().success().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.consistency().phase()).isEqualTo(ConsistencyPhase.LIVE);
        assertThat(response.consistency().verdict()).isNull();
        assertThat(response.snapshotAt()).isEqualTo(NOW);
    }

    /** Prometheus 가 죽어도 화면 전체가 죽으면 안 됩니다. */
    @Test
    @DisplayName("질의가 실패하면 예외가 아니라 UNAVAILABLE 로 나간다")
    void queryFailureBecomesUnavailable() {
        AdminMetricsResponse response = assemble(FakePromQuery.down(), globalQuery());

        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.latency().success().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.consistency().overIssued().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(response.consistency().severity()).isNull();
    }

    /**
     * 관측 시각을 모르면 값을 못 냅니다. 평가 시각으로 채우면 죽은 원천이 매 폴링마다 새로
     * 관측된 것처럼 보입니다.
     */
    @Test
    @DisplayName("신선도 미터가 없으면 값이 있어도 PENDING 이다")
    void unknownObservationTimeYieldsPending() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "app_consistency_gap_state", List.of(gap("lua", 3d), gapState("lua", SourceStatus.VALID))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.PENDING);
    }

    // ── 질의 ────────────────────────────────────────────────────────────────────

    /** 지표마다 한 번씩 부르면 1 초 폴링이 Prometheus 를 초당 수십 번 두드립니다. */
    @Test
    @DisplayName("셀렉터로 묶어 응답 한 장에 질의를 네 번만 보낸다")
    void usesSelectorsToLimitQueries() {
        FakePromQuery client = FakePromQuery.empty();
        assemble(client, globalQuery());

        assertThat(client.queries()).hasSize(4);
        assertThat(client.queries()).noneMatch(q -> q.contains("query_range"));
        // window 는 되돌아볼 범위가 아니라 비율을 계산할 집계 창이라 질의 안에 들어간다.
        assertThat(client.queries()).anyMatch(q -> q.contains("[60s]"));
        // 관측 시각은 표본이 아니라 timestamp() 로 따로 묻는다.
        assertThat(client.queries()).anyMatch(q -> q.contains("timestamp("));
    }

    /** 백분위 질의에는 창이 들어가지 않습니다 — expiry 가 정하는 값이라 PromQL 로 못 바꿉니다. */
    @Test
    @DisplayName("window 를 바꿔도 백분위 질의는 그대로다")
    void windowDoesNotReachPercentileQuery() {
        FakePromQuery oneMinute = FakePromQuery.empty();
        FakePromQuery fifteen = FakePromQuery.empty();
        assemble(oneMinute, globalQuery());
        assemble(fifteen, new MetricsQuery(MetricsWindow.FIFTEEN_MINUTES, null, null));

        assertThat(percentileQuery(oneMinute)).isEqualTo(percentileQuery(fifteen));
        assertThat(rateQuery(oneMinute)).isNotEqualTo(rateQuery(fifteen));
    }

    // ── 처리량 ──────────────────────────────────────────────────────────────────

    /** 결과 Counter 는 인스턴스 값을 더합니다. 발급 경로는 uri 그룹으로 먼저 쪼갠 뒤 집계합니다. */
    @Test
    @DisplayName("결과 rate 는 uri 그룹으로 쪼갠 뒤 인스턴스를 합산한다")
    void resultRatesSumAcrossInstances() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(
                        rate("issue", "success", 40d, "api-1"),
                        rate("issue", "success", 60d, "api-2"),
                        rate("queue", "success", 900d, "api-1"),
                        rate("issue", "policy_reject", 5d, "api-1"),
                        rate("entry", "queue_accepted", 7d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueSuccessTps().value()).isEqualTo(100d);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.VALID);
        // 순번 폴링(queue 그룹)이 발급 처리량에 섞이면 안 된다.
        assertThat(response.traffic().issueAttemptRps().value()).isEqualTo(105d);
        assertThat(response.traffic().queueAcceptedRps().value()).isEqualTo(7d);
        // 관측 시각은 평가 시각이 아니라 마지막 스크레이프 시각이다.
        assertThat(response.traffic().issueSuccessTps().observedAt())
                .isEqualTo(EVALUATED.minusSeconds(FRESH_AGE_SECONDS));
    }

    /** 요청이 없는 것은 장애가 아니라 상태입니다. */
    @Test
    @DisplayName("그룹 전체 rate 가 0이면 NO_TRAFFIC 으로 나간다")
    void zeroTotalRateIsNoTraffic() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.NO_TRAFFIC);
    }

    /**
     * 하위 분류가 0 인 것은 "요청이 없었다" 가 아니라 "그 결과가 한 건도 없었다" 입니다.
     * 둘을 같은 상태로 내보내면 초당 5,000 건이 전부 성공 중일 때 실패 패널만 회색으로 죽습니다.
     */
    @Test
    @DisplayName("트래픽이 있는데 하위 분류만 0이면 NO_TRAFFIC 이 아니라 VALID 다")
    void zeroSubclassWithTrafficIsValid() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(
                        rate("issue", "success", 5000d, "api-1"),
                        rate("issue", "application_failure", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.traffic().issueAttemptRps().state()).isEqualTo(SourceStatus.VALID);
        assertThat(response.traffic().systemFailureRps().value()).isEqualTo(0d);
        assertThat(response.traffic().systemFailureRps().state()).isEqualTo(SourceStatus.VALID);
    }

    /** 스크레이프가 멈춰도 instant query 는 5분간 값을 계속 돌려줍니다. */
    @Test
    @DisplayName("마지막 스크레이프가 오래되면 값을 싣되 STALE 로 내려보낸다")
    void oldScrapeIsStale() {
        FakePromQuery client = respond(Map.of(
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(STALE_AGE_SECONDS))));

        var success = assemble(client, globalQuery()).traffic().issueSuccessTps();

        assertThat(success.state()).isEqualTo(SourceStatus.STALE);
        assertThat(success.value()).isEqualTo(40d);
        assertThat(success.observedAt()).isEqualTo(EVALUATED.minusSeconds(STALE_AGE_SECONDS));
    }

    // ── 지연 ────────────────────────────────────────────────────────────────────

    /** 인스턴스별 백분위는 병합할 수 없습니다. 최댓값을 씁니다(DEC-02). */
    @Test
    @DisplayName("백분위는 인스턴스 최댓값을 쓰고 ms 로 환산한다")
    void percentilesTakeInstanceMax() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0.010d, "api-1"),
                        quantile("0.5", 0.020d, "api-2"),
                        quantile("0.95", 0.100d, "api-1"),
                        quantile("0.99", 0.250d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        var success = assemble(client, globalQuery()).latency().success();

        assertThat(success.state()).isEqualTo(SourceStatus.VALID);
        assertThat(success.value().p50Millis()).isEqualTo(20d);
        assertThat(success.value().p95Millis()).isEqualTo(100d);
        assertThat(success.value().p99Millis()).isEqualTo(250d);
    }

    /** expiry 로 창이 비면 백분위가 0 으로 읽힙니다. 그대로 내보내면 "지연 0ms" 를 그립니다. */
    @Test
    @DisplayName("관측 창이 비어 백분위가 0이면 0ms 가 아니라 PENDING 이다")
    void zeroPercentileIsPendingNotZeroLatency() {
        FakePromQuery client = respond(Map.of(
                "quantile!=", List.of(
                        quantile("0.5", 0d, "api-1"),
                        quantile("0.95", 0d, "api-1"),
                        quantile("0.99", 0d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        var success = assemble(client, globalQuery()).latency().success();

        assertThat(success.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(success.value()).isNull();
    }

    // ── 정합성 ──────────────────────────────────────────────────────────────────

    /** 값 미터와 상태 미터는 짝입니다. 상태가 값을 요구할 때만 값을 싣습니다. */
    @Test
    @DisplayName("정합성 gap 은 값 미터와 상태 미터를 짝으로 읽는다")
    void consistencyPairsValueAndState() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        gap("active_db", Double.NaN),
                        gapState("active_db", SourceStatus.N_A),
                        gap("persist", Double.NaN),
                        gapState("persist", SourceStatus.UNAVAILABLE),
                        domain(MetricAggregation.CONSISTENCY_SEVERITY,
                                live(), SourceStatusCode.of(Severity.WARN)),
                        domain(MetricAggregation.CONSISTENCY_SEVERITY_STATE,
                                live(), SourceStatusCode.of(SourceStatus.VALID)),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var consistency = assemble(client, globalQuery()).consistency();

        assertThat(consistency.luaGap().value()).isEqualTo(3L);
        assertThat(consistency.luaGap().state()).isEqualTo(SourceStatus.VALID);
        // 관측 시각은 batch 의 마지막 수집 성공 시각이다. 평가 시각(=지금)이 아니다.
        assertThat(consistency.luaGap().observedAt())
                .isEqualTo(EVALUATED.minusSeconds(FRESH_AGE_SECONDS));
        // batch 가 실어 보낸 이유는 그대로 나간다. N_A 도 UNAVAILABLE 도 0 이 아니다.
        assertThat(consistency.activeDbGap().state()).isEqualTo(SourceStatus.N_A);
        assertThat(consistency.persistGap().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        // 상태 미터가 아예 없는 gap 은 PENDING 이다.
        assertThat(consistency.dbCounterGap().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(consistency.severity()).isEqualTo(Severity.WARN);
    }

    /**
     * 상태 VALID + 값 NaN 은 scrape 중 한 틱 어긋난 것이지 장애가 아닙니다
     * ({@code DomainGaugeRegistrar} 가 남는 창으로 문서화한 상태).
     */
    @Test
    @DisplayName("상태는 값을 요구하는데 값이 NaN 이면 장애색이 아니라 표시 없음이다")
    void oneTickSkewIsPendingNotUnavailable() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", Double.NaN),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.PENDING);
        assertThat(luaGap.value()).isNull();
    }

    /** batch 수집이 오래 멈추면 gap 값이 남아 있어도 실시간 값으로 읽히면 안 됩니다. */
    @Test
    @DisplayName("batch 수집 성공이 오래되면 gap 이 STALE 로 나간다")
    void oldCollectionIsStale() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(STALE_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.STALE);
        assertThat(luaGap.value()).isEqualTo(3L);
    }

    /**
     * 원천이 하나여야 하는 지표에 표본이 둘이면 어느 쪽도 고를 수 없습니다. 그렇다고 응답 전체를
     * 500 으로 날리면 batch 를 두 대로 늘리거나 scrape 대상을 추가한 순간 관제가 통째로 죽습니다.
     */
    @Test
    @DisplayName("SINGLE 규칙이 깨져도 그 값만 UNAVAILABLE 이고 응답은 살아남는다")
    void brokenSingleRuleDoesNotKillTheResponse() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        domain(MetricAggregation.CONSISTENCY_GAP,
                                Map.of(DomainMeterNames.TAG_GAP_TYPE, "lua",
                                        DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE,
                                        "instance", "batch-2"), 9d),
                        collectSuccess(FRESH_AGE_SECONDS)),
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.UNAVAILABLE);
        // 무관한 지표는 멀쩡해야 한다.
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.VALID);
    }

    /** batch 가 다른 회차를 보고 있으면 그 값은 요청한 회차의 값이 아닙니다. */
    @Test
    @DisplayName("요청 쿠폰과 batch 가 관측 중인 쿠폰이 다르면 N_A 로 나간다")
    void mismatchedCouponScopeIsNotApplicable() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        domain(MetricAggregation.CONSISTENCY_COUPON_ID, Map.of(), 77),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var response = assemble(client, new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null));

        assertThat(response.scope().couponId()).isEqualTo(42L);
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.N_A);
        assertThat(response.consistency().luaGap().value()).isNull();
    }


    /**
     * FINAL 판정은 조용해진 뒤 검증 배치가 같은 미터 이름으로 냅니다. 라벨로 가르지 않으면
     * 읽는 쪽이 둘을 같은 것으로 읽습니다.
     */
    @Test
    @DisplayName("phase=live 가 아닌 표본은 정합성 값에 섞이지 않는다")
    void finalPhaseSamplesAreNotMixedIntoLive() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        domain(MetricAggregation.CONSISTENCY_GAP,
                                Map.of(DomainMeterNames.TAG_GAP_TYPE, "lua",
                                        DomainMeterNames.TAG_PHASE, "final"), 999d),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.VALID);
        assertThat(luaGap.value()).isEqualTo(3L);
    }

    /**
     * 신선도 미터에 표본이 둘이면 어느 batch 의 시각인지 못 고릅니다. 그렇다고 응답 전체를
     * 500 으로 날리면 batch 를 두 대로 늘리는 순간 관제가 통째로 죽습니다.
     */
    @Test
    @DisplayName("신선도 미터의 SINGLE 규칙이 깨져도 응답은 살아남는다")
    void brokenFreshnessRuleDoesNotKillTheResponse() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS),
                        domain(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                                Map.of(DomainMeterNames.TAG_COLLECT_PATH,
                                        DomainMeterNames.PATH_CONSISTENCY, "instance", "batch-2"),
                                EVALUATED.minusSeconds(9).getEpochSecond())),
                "rate(", List.of(rate("issue", "success", 40d, "api-1")),
                "timestamp(", List.of(age(FRESH_AGE_SECONDS))));

        AdminMetricsResponse response = assemble(client, globalQuery());

        // 관측 시각을 모르므로 표시 없음이지만, 무관한 지표는 멀쩡하다.
        assertThat(response.consistency().luaGap().state()).isEqualTo(SourceStatus.PENDING);
        assertThat(response.traffic().issueSuccessTps().state()).isEqualTo(SourceStatus.VALID);
    }

    /**
     * PENDING("아직 안 나옴, 기다려라")과 UNAVAILABLE("원천이 죽었다, 조치하라")은 운영자가
     * 취할 행동이 정반대입니다.
     */
    @Test
    @DisplayName("신선도 질의만 실패하면 PENDING 이 아니라 UNAVAILABLE 이다")
    void freshnessQueryFailureIsUnavailableNotPending() {
        FakePromQuery client = new FakePromQuery(promQl -> {
            if (promQl.contains("timestamp(")) {
                throw new PromQueryException("시험용 장애");
            }
            if (promQl.startsWith("rate(")) {
                return List.of(rate("issue", "success", 40d, "api-1"));
            }
            return List.of();
        });

        var success = assemble(client, globalQuery()).traffic().issueSuccessTps();

        assertThat(success.state()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 낡은 판정을 지금 판정처럼 내보내면 화면이 초록불을 그립니다. */
    @Test
    @DisplayName("severity 가 낡았으면 값을 내지 않는다")
    void staleSeverityIsNotReported() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        domain(MetricAggregation.CONSISTENCY_SEVERITY,
                                live(), SourceStatusCode.of(Severity.NONE)),
                        domain(MetricAggregation.CONSISTENCY_SEVERITY_STATE,
                                live(), SourceStatusCode.of(SourceStatus.VALID)),
                        collectSuccess(STALE_AGE_SECONDS))));

        assertThat(assemble(client, globalQuery()).consistency().severity()).isNull();
    }

    /** 무한대는 값이 아닙니다. Long.MAX_VALUE 로 응답에 실리면 KPI 가 그대로 거짓 알람이 됩니다. */
    @Test
    @DisplayName("무한대 표본은 값이 아니라 표시 없음으로 처리한다")
    void infiniteSampleIsNotAValue() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", Double.POSITIVE_INFINITY),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, globalQuery()).consistency().luaGap();

        assertThat(luaGap.value()).isNull();
        assertThat(luaGap.state()).isEqualTo(SourceStatus.PENDING);
    }

    /** 어느 회차를 본 값인지 모르면 그 회차의 값이라고 말할 수 없습니다. */
    @Test
    @DisplayName("COUPON 범위인데 관측 회차를 모르면 N_A 다")
    void unknownObservedCouponIsNotApplicable() {
        FakePromQuery client = respond(Map.of(
                "app_consistency_gap_state", List.of(
                        gap("lua", 3d),
                        gapState("lua", SourceStatus.VALID),
                        collectSuccess(FRESH_AGE_SECONDS))));

        var luaGap = assemble(client, new MetricsQuery(MetricsWindow.ONE_MINUTE, 42L, null))
                .consistency().luaGap();

        assertThat(luaGap.state()).isEqualTo(SourceStatus.N_A);
    }

    // ── 도우미 ─────────────────────────────────────────────────────────────────

    private static AdminMetricsResponse assemble(PromQuery client, MetricsQuery query) {
        return new PromMetricsAssembler(client, FIXED_TIME, STALE_AFTER).assemble(query);
    }

    private static MetricsQuery globalQuery() {
        return new MetricsQuery(MetricsWindow.ONE_MINUTE, null, null);
    }

    private static String percentileQuery(FakePromQuery client) {
        return client.queries().stream().filter(q -> q.contains("quantile!=")).findFirst().orElseThrow();
    }

    private static String rateQuery(FakePromQuery client) {
        return client.queries().stream().filter(q -> q.startsWith("rate(")).findFirst().orElseThrow();
    }

    /** 질의 문자열에 포함된 조각으로 응답을 고르는 fake 를 만듭니다. */
    private static FakePromQuery respond(Map<String, List<PromSample>> byQueryFragment) {
        return new FakePromQuery(promQl -> {
            List<PromSample> matched = new ArrayList<>();
            byQueryFragment.forEach((fragment, samples) -> {
                if (promQl.contains(fragment)) {
                    matched.addAll(samples);
                }
            });
            return List.copyOf(matched);
        });
    }

    private static PromSample rate(String uriGroup, String result, double value, String instance) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("uri_group", uriGroup);
        labels.put("result", result);
        labels.put("instance", instance);
        // rate() 결과에는 __name__ 이 없다. 실제 Prometheus 응답과 같게 둔다.
        return new PromSample("", labels, value, EVALUATED);
    }

    private static PromSample quantile(String quantile, double seconds, String instance) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("uri_group", "issue");
        labels.put("outcome", "success");
        labels.put("quantile", quantile);
        labels.put("instance", instance);
        return new PromSample(MetricAggregation.HTTP_LATENCY_SECONDS, labels, seconds, EVALUATED);
    }

    /** {@code max(time() - timestamp(...))} 결과. 라벨도 __name__ 도 없다. */
    private static PromSample age(long ageSeconds) {
        return new PromSample("", Map.of(), ageSeconds, EVALUATED);
    }

    private static PromSample collectSuccess(long ageSeconds) {
        return domain(MetricAggregation.COLLECT_LAST_SUCCESS_EPOCH,
                Map.of(DomainMeterNames.TAG_COLLECT_PATH, DomainMeterNames.PATH_CONSISTENCY),
                EVALUATED.minusSeconds(ageSeconds).getEpochSecond());
    }

    /** batch 의 evaluationGauge 는 평가 미터에 phase=live 를 붙인다. */
    private static Map<String, String> liveGap(String type) {
        return Map.of(DomainMeterNames.TAG_GAP_TYPE, type,
                DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE);
    }

    private static Map<String, String> live() {
        return Map.of(DomainMeterNames.TAG_PHASE, DomainMeterNames.PHASE_LIVE);
    }

    private static PromSample gap(String type, double value) {
        return domain(MetricAggregation.CONSISTENCY_GAP, liveGap(type), value);
    }

    private static PromSample gapState(String type, SourceStatus status) {
        return domain(MetricAggregation.CONSISTENCY_GAP_STATE, liveGap(type),
                SourceStatusCode.of(status));
    }

    private static PromSample domain(String metricName, Map<String, String> labels, double value) {
        return new PromSample(metricName, labels, value, EVALUATED);
    }
}

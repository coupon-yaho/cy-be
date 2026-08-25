package com.kafkick.api.admin.observability;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import java.time.Clock;
import java.util.List;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import com.kafkick.api.admin.events.LiveEventAssembler;
import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.observation.attempt.AttemptLiveEntry;
import com.kafkick.core.observation.attempt.AttemptLivePage;
import com.kafkick.core.observation.attempt.AttemptLiveReader;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.support.TimeProvider;

/** 연결된 관리자 지표 조회와 live event polling 의 HTTP 계약을 검증합니다. */
class AdminObservabilityControllerTest {

    private static final TimeProvider FIXED_TIME =
            new TimeProvider(Clock.fixed(Instant.parse("2026-08-21T00:00:00Z"), ZoneOffset.UTC));
    private static final Duration STALE_AFTER = Duration.ofSeconds(120);
    private static final Duration BUDGET = Duration.ofMillis(900);

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminObservabilityController(
                    new PromMetricsAssembler(FakePromQuery.empty(), FIXED_TIME, STALE_AFTER, BUDGET),
                    new PromSeriesAssembler(FakePromRangeQuery.alwaysOnePoint(), FIXED_TIME,
                            PrometheusSeriesProperties.defaults()),
                    emptyLiveEvents()));

    /** 유효한 집계 구간과 단일 관측 범위가 바인딩된 뒤 값마다 상태가 붙은 스냅샷이 나가는지 검증합니다. */
    @Test
    @DisplayName("관측 지표 조회는 유효 window 요청에 범위를 되비친 스냅샷을 반환한다")
    void metricsReturnsSnapshot() throws Exception {
        mockMvc.perform(get("/api/v1/admin/metrics").param("window", "1m").param("couponId", "1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.scope.type").value("COUPON"))
                .andExpect(jsonPath("$.data.scope.couponId").value(1))
                .andExpect(jsonPath("$.data.window").value("ONE_MINUTE"))
                // 표본이 없는 구간은 0 이 아니라 PENDING 이다.
                .andExpect(jsonPath("$.data.traffic.issueAttemptRps.state").value("PENDING"))
                .andExpect(jsonPath("$.data.traffic.issueAttemptRps.value").doesNotExist());
    }

    /** 허용값 밖의 window 는 조회를 시도하기 전에 400 으로 끊긴다. */
    @Test
    @DisplayName("관측 지표 조회는 허용값 밖 window를 400으로 거부한다")
    void metricsRejectUnsupportedWindow() throws Exception {
        mockMvc.perform(get("/api/v1/admin/metrics").param("window", "24h"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** Prometheus 가 죽어도 화면 전체가 죽으면 안 된다. */
    @Test
    @DisplayName("Prometheus 질의가 실패해도 500이 아니라 state=UNAVAILABLE 로 나간다")
    void metricsSurviveDownstreamFailure() throws Exception {
        MockMvc failing = AdminControllerContractTestSupport.mockMvc(
                new AdminObservabilityController(
                        new PromMetricsAssembler(FakePromQuery.down(), FIXED_TIME, STALE_AFTER, BUDGET),
                        new PromSeriesAssembler(FakePromRangeQuery.down(), FIXED_TIME,
                                PrometheusSeriesProperties.defaults()),
                    emptyLiveEvents()));

        failing.perform(get("/api/v1/admin/metrics").param("window", "1m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.traffic.issueAttemptRps.state").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.data.consistency.luaGap.state").value("UNAVAILABLE"));
    }

    /**
     * NaN 표본은 "0%" 가 아니라 "계산할 수 없다" 이다. 실제 운영 JSON 정책(non_null)이 키를
     * 지우면 화면이 undefined 를 읽고 0 으로 그린다 — 값 없음이 정상 0 으로 둔갑한다.
     */
    @Test
    @DisplayName("시계열의 NaN 표본은 non_null 정책에서도 value 키를 null 로 유지한다")
    void seriesKeepsNullValueKeyUnderNonNullPolicy() throws Exception {
        MockMvc nonNull = AdminControllerContractTestSupport.mockMvcWithNonNullJson(
                new AdminObservabilityController(
                        new PromMetricsAssembler(FakePromQuery.empty(), FIXED_TIME, STALE_AFTER, BUDGET),
                        new PromSeriesAssembler(FakePromRangeQuery.withNaNFirstPoint(), FIXED_TIME,
                                PrometheusSeriesProperties.defaults()),
                    emptyLiveEvents()));

        nonNull.perform(get("/api/v1/admin/metrics/series").param("window", "1m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.series[0].points[0].value").doesNotExist())
                .andExpect(jsonPath("$.data.series[0].points[0]").value(
                        org.hamcrest.Matchers.hasKey("value")))
                .andExpect(jsonPath("$.data.series[0].points[1].value").value(42.0));
    }

    /** 지원하지 않는 범위를 조용히 무시하면 화면이 전역 값을 회차 값으로 읽는다. */
    @Test
    @DisplayName("시계열 조회는 아직 지원하지 않는 범위 파라미터를 400으로 거부한다")
    void seriesRejectsUnsupportedScopeParameters() throws Exception {
        mockMvc.perform(get("/api/v1/admin/metrics/series")
                        .param("window", "1m").param("couponId", "5"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("ADMIN-004"));
        mockMvc.perform(get("/api/v1/admin/metrics/series")
                        .param("window", "1m").param("benchmarkRunId", "7"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error.code").value("ADMIN-004"));
    }

    /** 쿠폰 범위와 Benchmark 범위를 동시에 지정한 요청을 400으로 거부하는지 검증합니다. */
    @Test
    @DisplayName("관측 지표 조회는 couponId와 benchmarkRunId를 함께 받으면 400 실패 봉투를 반환한다")
    void metricsRejectMutuallyExclusiveScopes() throws Exception {
        mockMvc.perform(get("/api/v1/admin/metrics")
                        .param("window", "1m")
                        .param("couponId", "1")
                        .param("benchmarkRunId", "2"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** live event 조회가 과거용 beforeCursor가 아닌 afterCursor를 사용하는지 검증합니다. */
    @Test
    @DisplayName("관측 이벤트 조회는 afterCursor와 limit을 바인딩해 항목을 반환한다")
    void eventsReturnBufferedItems() throws Exception {
        MockMvc withEvents = AdminControllerContractTestSupport.mockMvc(controller(
                page(List.of(entry()), "1723520000000-3", true, false)));

        withEvents.perform(get("/api/v1/admin/events")
                        .param("afterCursor", "1723519999999-0").param("limit", "50"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.items[0].eventId")
                        .value("11111111-1111-1111-1111-111111111111"))
                .andExpect(jsonPath("$.data.hasMore").value(true))
                .andExpect(jsonPath("$.data.cursorReset").value(false));
    }

    /**
     * <b>커서는 문자열로 나가야 한다.</b> Stream ID 의 밀리초 부분만으로도 JS 의 안전 정수
     * 범위(2^53-1)를 넘는다. 숫자로 나가면 화면이 조용히 반올림하고, 그 커서로는 다시 조회할
     * 수 없다 — 예외 없이 폴링만 어긋난다.
     */
    @Test
    @DisplayName("nextAfterCursor 는 숫자가 아니라 불투명 문자열로 나간다")
    void eventsExposeTheCursorAsAnOpaqueString() throws Exception {
        MockMvc withEvents = AdminControllerContractTestSupport.mockMvc(controller(
                page(List.of(entry()), "1723520000000-3", false, false)));

        withEvents.perform(get("/api/v1/admin/events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.nextAfterCursor").value("1723520000000-3"))
                .andExpect(jsonPath("$.data.nextAfterCursor").value(
                        org.hamcrest.Matchers.instanceOf(String.class)));
    }

    /** 만료된 커서에서 500 이나 빈 응답이 아니라 200 + 복구 플래그가 나간다. */
    @Test
    @DisplayName("만료 커서는 200 으로 복구 플래그와 함께 나간다")
    void eventsReportCursorResetInsteadOfFailing() throws Exception {
        MockMvc expired = AdminControllerContractTestSupport.mockMvc(controller(
                page(List.of(entry()), "1723520000000-3", false, true)));

        expired.perform(get("/api/v1/admin/events").param("afterCursor", "0-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.cursorReset").value(true))
                .andExpect(jsonPath("$.data.eventsMayBeMissing").value(true))
                .andExpect(jsonPath("$.data.items").isNotEmpty());
    }

    /** limit 경계는 그대로다 — 원천이 붙었다고 400 계약이 흔들리면 안 된다. */
    @Test
    @DisplayName("관측 이벤트 조회는 limit 경계를 400으로 거부한다")
    void eventsRejectLimitsOutsideTheContract() throws Exception {
        mockMvc.perform(get("/api/v1/admin/events").param("limit", "0"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/v1/admin/events").param("limit", "201"))
                .andExpect(status().isBadRequest());
    }

    private static AdminObservabilityController controller(AttemptLivePage page) {
        return new AdminObservabilityController(
                new PromMetricsAssembler(FakePromQuery.empty(), FIXED_TIME, STALE_AFTER, BUDGET),
                new PromSeriesAssembler(FakePromRangeQuery.alwaysOnePoint(), FIXED_TIME,
                        PrometheusSeriesProperties.defaults()),
                new LiveEventAssembler(reader(page)));
    }

    private static LiveEventAssembler emptyLiveEvents() {
        return new LiveEventAssembler(reader(page(List.of(), null, false, false)));
    }

    private static AttemptLivePage page(
            List<AttemptLiveEntry> entries, String nextCursor, boolean hasMore, boolean expired) {
        return new AttemptLivePage(entries, nextCursor, hasMore, expired);
    }

    private static AttemptLiveReader reader(AttemptLivePage page) {
        return (afterCursor, limit) -> page;
    }

    private static AttemptLiveEntry entry() {
        return new AttemptLiveEntry(
                java.util.UUID.fromString("11111111-1111-1111-1111-111111111111"),
                EventType.ISSUE_ATTEMPT, 101L, 201L, null, null, null, null, null, null, null,
                false, Instant.parse("2026-08-25T00:00:00Z"), Instant.parse("2026-08-25T00:00:01Z"));
    }
}

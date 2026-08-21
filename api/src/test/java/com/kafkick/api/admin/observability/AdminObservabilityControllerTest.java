package com.kafkick.api.admin.observability;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.support.TimeProvider;

/** 연결된 관리자 지표 조회와 아직 선구축 상태인 live event polling의 계약을 검증합니다. */
class AdminObservabilityControllerTest {

    private static final TimeProvider FIXED_TIME =
            new TimeProvider(Clock.fixed(Instant.parse("2026-08-21T00:00:00Z"), ZoneOffset.UTC));
    private static final Duration STALE_AFTER = Duration.ofSeconds(120);

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminObservabilityController(
                    new PromMetricsAssembler(FakePromQuery.empty(), FIXED_TIME, STALE_AFTER)));

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
                        new PromMetricsAssembler(FakePromQuery.down(), FIXED_TIME, STALE_AFTER)));

        failing.perform(get("/api/v1/admin/metrics").param("window", "1m"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.traffic.issueAttemptRps.state").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.data.consistency.luaGap.state").value("UNAVAILABLE"));
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
    @DisplayName("관측 이벤트 조회는 afterCursor와 limit을 바인딩하고 ADMIN-001 선구축 오류를 반환한다")
    void eventsReturnNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/events").param("afterCursor", "cursor").param("limit", "50"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }
}

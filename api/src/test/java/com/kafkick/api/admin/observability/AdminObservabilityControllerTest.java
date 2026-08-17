package com.kafkick.api.admin.observability;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;

/** 관리자 지표와 live event polling의 범위·cursor 계약을 검증합니다. */
class AdminObservabilityControllerTest {

    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(new AdminObservabilityController());

    /** 유효한 집계 구간과 단일 관측 범위가 바인딩된 뒤 501로 도달하는지 검증합니다. */
    @Test
    @DisplayName("관측 지표 조회는 유효 window 요청에 ADMIN-001 선구축 오류를 반환한다")
    void metricsReturnsNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/metrics").param("window", "1m").param("couponId", "1"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
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

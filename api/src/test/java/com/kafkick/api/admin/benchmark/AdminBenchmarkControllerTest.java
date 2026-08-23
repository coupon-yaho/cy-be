package com.kafkick.api.admin.benchmark;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import static org.mockito.Mockito.mock;
import java.util.Optional;

/** Benchmark 조회와 네 가지 운영 명령의 독립 HTTP 계약 및 Validation을 검증합니다. */
class AdminBenchmarkControllerTest {

    private final RunTimeseriesArchiver archiver = mock(RunTimeseriesArchiver.class);
    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminBenchmarkController(Optional.of(archiver)));

    /** 유효한 과거 방향 목록 조건은 실제 저장소 대신 명시적인 ADMIN-001을 반환해야 합니다. */
    @Test
    @DisplayName("벤치마크 목록 조회는 beforeCursor와 limit을 바인딩하고 ADMIN-001 선구축 오류를 반환한다")
    void benchmarksReturnNotImplementedEnvelope() throws Exception {
        mockMvc.perform(get("/api/v1/admin/benchmarks")
                        .param("from", "2026-01-01")
                        .param("to", "2026-08-16")
                        .param("beforeCursor", "cursor")
                        .param("limit", "50"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** 목록 크기가 공통 최댓값 200을 초과하면 Controller 진입 전에 400으로 거부되는지 확인합니다. */
    @Test
    @DisplayName("벤치마크 목록 조회는 limit 201을 400 실패 봉투로 거부한다")
    void benchmarksRejectLimitAboveMaximum() throws Exception {
        mockMvc.perform(get("/api/v1/admin/benchmarks").param("limit", "201"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 시작일이 종료일보다 늦은 Benchmark 기간 필터를 400으로 거부하는지 확인합니다. */
    @Test
    @DisplayName("벤치마크 목록 조회는 역전된 기간을 400 실패 봉투로 거부한다")
    void benchmarksRejectReversedRange() throws Exception {
        mockMvc.perform(get("/api/v1/admin/benchmarks")
                        .param("from", "2026-08-16")
                        .param("to", "2026-01-01"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.success").value(false));
    }

    /** 상세·시작·중지·FINAL 확정·k6 업로드가 합쳐지지 않은 별도 endpoint인지 검증합니다. */
    @Test
    void exposesDetailAndFourIndependentBenchmarkOperations() throws Exception {
        mockMvc.perform(get("/api/v1/admin/benchmarks/1"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"engineVersion\":\"V3\",\"releaseStage\":\"V3\","
                                + "\"queueMode\":\"ADAPTIVE\",\"scenarioCode\":\"LOAD_100K\"}"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/stop"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/finalize"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/k6-result")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"tps\":1847.2,\"p99Millis\":412.3,\"failureCount\":0,"
                                + "\"failureRate\":0.0,\"measuredAt\":\"2026-08-16T00:00:00Z\"}"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    /** k6 실패율의 확정 범위인 0~1을 벗어난 요청을 400으로 거부하는지 검증합니다. */
    @Test
    @DisplayName("k6 실패 비율은 0 이상 1 이하만 허용한다")
    void k6ResultRejectsFailureRateAboveOne() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/k6-result")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"tps\":1.0,\"p99Millis\":1.0,\"failureCount\":2,"
                                + "\"failureRate\":1.1,\"measuredAt\":\"2026-08-16T00:00:00Z\"}"))
                .andExpect(status().isBadRequest());
    }

    /** 알 수 없는 enum을 시작 요청에서 거부합니다. */
    @Test
    void benchmarkStartRejectsUnknownEnums() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"engineVersion\":\"UNKNOWN\",\"releaseStage\":\"V3\","
                                + "\"queueMode\":\"ADAPTIVE\",\"scenarioCode\":\"LOAD_100K\"}"))
                .andExpect(status().isBadRequest());
    }

    /** 허용 형식에 맞지 않는 시나리오 코드를 시작 요청에서 독립적으로 거부합니다. */
    @Test
    void benchmarkStartRejectsInvalidScenarioCode() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"engineVersion\":\"V3\",\"releaseStage\":\"V3\","
                                + "\"queueMode\":\"ADAPTIVE\",\"scenarioCode\":\"lower case\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void retryArchiveInvokesArchiver() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/7/archive/retry"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
        verify(archiver).retry(7L);
    }

    @Test
    void retryArchiveRejectsNonPositiveIdBeforeInvocation() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/0/archive/retry"))
                .andExpect(status().isBadRequest());
        verify(archiver, never()).retry(0L);
    }

    @Test
    void retryArchiveReturnsServerErrorWhenFeatureIsUnavailable() throws Exception {
        MockMvc unavailable = AdminControllerContractTestSupport.mockMvc(
                new AdminBenchmarkController(Optional.empty()));
        unavailable.perform(post("/api/v1/admin/benchmarks/7/archive/retry"))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.success").value(false));
    }
}

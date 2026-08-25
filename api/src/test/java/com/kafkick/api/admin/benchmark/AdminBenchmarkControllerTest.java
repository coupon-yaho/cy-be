package com.kafkick.api.admin.benchmark;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.mockito.BDDMockito.given;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import com.kafkick.api.admin.support.AdminControllerContractTestSupport;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.BenchmarkErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.api.admin.benchmark.dto.BenchmarkCommandAcceptedResponse;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.benchmark.BenchmarkArchiveStatus;

/** Benchmark 조회와 네 가지 운영 명령의 독립 HTTP 계약 및 Validation을 검증합니다. */
class AdminBenchmarkControllerTest {

    private final RunTimeseriesArchiver archiver = mock(RunTimeseriesArchiver.class);
    private final BenchmarkStartOrchestrator startOrchestrator = mock(BenchmarkStartOrchestrator.class);
    private final BenchmarkFinalizeOrchestrator finalizeOrchestrator = mock(BenchmarkFinalizeOrchestrator.class);
    private final MockMvc mockMvc = AdminControllerContractTestSupport.mockMvc(
            new AdminBenchmarkController(Optional.of(archiver), startOrchestrator, finalizeOrchestrator));

    @Test
    @DisplayName("replica 수는 양수 범위만 HTTP에서 받고 배포값 일치는 gate가 검사한다")
    void startRejectsOutOfRangeReplicaCount() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validStartJson().replace("\"appReplicas\":4", "\"appReplicas\":0")))
            .andExpect(status().isBadRequest());
        verify(startOrchestrator, never()).start(any(), any());
    }

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
        given(startOrchestrator.start(any(), any())).willReturn(new BenchmarkCommandAcceptedResponse(
            91L, BenchmarkRunState.RUNNING, Instant.parse("2026-08-23T01:02:03Z")));
        given(finalizeOrchestrator.finalizeRun(1L)).willReturn(new BenchmarkCommandAcceptedResponse(
            1L, BenchmarkRunState.FINALIZED, Instant.parse("2026-08-23T01:03:09Z"),
            BenchmarkArchiveStatus.FAILED));
        mockMvc.perform(get("/api/v1/admin/benchmarks/1"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validStartJson()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.benchmarkRunId").value(91))
                .andExpect(jsonPath("$.data.state").value("RUNNING"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/stop"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/finalize"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.state").value("FINALIZED"))
                .andExpect(jsonPath("$.data.archiveStatus").value("FAILED"));
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/client-result")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestCount\":100,\"failureCount\":0,\"droppedIterations\":0,"
                                + "\"tps\":1847.2,\"p95Millis\":206.1,\"p99Millis\":412.3,"
                                + "\"measuredAt\":\"2026-08-16T00:00:00Z\"}"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    @Test
    void finalizeMatrixParameterStillRequiresCommandSecret() throws Exception {
        MockMvc withoutSecret = AdminControllerContractTestSupport.mockMvcWithoutAdminHeaders(
            new AdminBenchmarkController(Optional.of(archiver), startOrchestrator, finalizeOrchestrator));

        withoutSecret.perform(post("/api/v1/admin/benchmarks/7/finalize;a=b")
                .header(com.kafkick.api.caller.HeaderCallerResolver.USER_ID_HEADER, "812934")
                .header(com.kafkick.api.admin.support.AdminAuthorizationInterceptor.USER_ROLE_HEADER, "ADMIN"))
            .andExpect(status().isForbidden());
        verify(finalizeOrchestrator, never()).finalizeRun(7L);
    }

    /** 실패 건수가 전체 요청 수를 넘는 공식 결과를 400으로 거부하는지 검증합니다. */
    @Test
    @DisplayName("k6 실패 비율은 0 이상 1 이하만 허용한다")
    void clientResultRejectsFailureCountAboveRequestCount() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/1/client-result")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"requestCount\":1,\"failureCount\":2,\"droppedIterations\":0,"
                                + "\"tps\":1.0,\"p95Millis\":1.0,\"p99Millis\":1.0,"
                                + "\"measuredAt\":\"2026-08-16T00:00:00Z\"}"))
                .andExpect(status().isBadRequest());
    }

    /** 알 수 없는 enum을 시작 요청에서 거부합니다. */
    @Test
    void benchmarkStartRejectsUnknownEnums() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validStartJson().replace("\"engineVersion\":\"V3\"",
                            "\"engineVersion\":\"UNKNOWN\"")))
                .andExpect(status().isBadRequest());
    }

    /** 허용 형식에 맞지 않는 시나리오 코드를 시작 요청에서 독립적으로 거부합니다. */
    @Test
    void benchmarkStartRejectsInvalidScenarioCode() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validStartJson().replace("\"scenarioCode\":\"LOAD_100K\"",
                            "\"scenarioCode\":\"lower case\"")))
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
        mockMvc.perform(post("/api/v1/admin/benchmarks/-1/archive/retry"))
                .andExpect(status().isBadRequest());
        verify(archiver, never()).retry(0L);
        verify(archiver, never()).retry(-1L);
    }

    @Test
    void retryArchiveReturnsNotImplementedWhenFeatureIsUnavailable() throws Exception {
        MockMvc unavailable = AdminControllerContractTestSupport.mockMvc(
                new AdminBenchmarkController(Optional.empty(), startOrchestrator, finalizeOrchestrator));
        unavailable.perform(post("/api/v1/admin/benchmarks/7/archive/retry"))
                .andExpect(status().isNotImplemented())
                .andExpect(jsonPath("$.error.code").value("ADMIN-001"));
    }

    @Test
    void retryArchiveMapsMissingRunToNotFound() throws Exception {
        doThrow(new BusinessException(BenchmarkErrorCode.RUN_NOT_FOUND, "benchmarkRunId=404"))
                .when(archiver).retry(404L);

        mockMvc.perform(post("/api/v1/admin/benchmarks/404/archive/retry"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error.code").value("BENCHMARK-003"));
    }

    @Test
    void retryArchiveMapsInvalidStateToConflict() throws Exception {
        doThrow(new BusinessException(BenchmarkErrorCode.ILLEGAL_TRANSITION, "archiveStatus=DONE"))
                .when(archiver).retry(7L);

        mockMvc.perform(post("/api/v1/admin/benchmarks/7/archive/retry"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error.code").value("BENCHMARK-004"));
    }

    @Test
    void benchmarkStartRejectsReplicaCountThatCanOverflowTopologyTotals() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validStartJson().replace("\"appReplicas\":4", "\"appReplicas\":2147483647")))
            .andExpect(status().isBadRequest());
    }

    @Test
    void benchmarkStartDelegatesObservationWindowWithoutBeanValidationRejection() throws Exception {
        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validStartJson().replace(
                    "\"observationHoldSeconds\":60", "\"observationHoldSeconds\":61")))
            .andExpect(status().isOk());
        org.mockito.Mockito.verify(startOrchestrator).start(any(), any());
    }

    @Test
    void benchmarkStartReturnsActionableTopologyViolations() throws Exception {
        doThrow(new TopologyValidationException(java.util.List.of(
            new BatchTopologyPreflight.Violation(
                "hikari.pool.total", "12", "10", "pool mismatch"))))
            .when(startOrchestrator).start(any(), any());

        mockMvc.perform(post("/api/v1/admin/benchmarks/start")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validStartJson()))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error.code").value("BENCHMARK-008"))
            .andExpect(jsonPath("$.error.details[0].key").value("hikari.pool.total"))
            .andExpect(jsonPath("$.error.details[0].expected").value("12"))
            .andExpect(jsonPath("$.error.details[0].actual").value("10"))
            .andExpect(jsonPath("$.error.details[0].reason").value("pool mismatch"));
    }

    private static String validStartJson() {
        return "{\"runKey\":\"V3-MAIN-01\",\"runType\":\"MAIN\","
            + "\"engineVersion\":\"V3\",\"releaseStage\":\"V3\","
            + "\"queueMode\":\"ADAPTIVE\",\"scenarioCode\":\"LOAD_100K\","
            + "\"couponId\":10,\"appReplicas\":4,\"offeredRps\":20000,"
            + "\"loadHoldSeconds\":5,\"observationHoldSeconds\":60,\"stockTotal\":10000,"
            + "\"generatorIdleRttMillis\":0.4,\"loadTool\":\"k6\","
            + "\"loadToolVersion\":\"1.0\",\"loadScriptHash\":\"" + "a".repeat(64) + "\"}";
    }
}

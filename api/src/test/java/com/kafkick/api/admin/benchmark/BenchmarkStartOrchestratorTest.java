package com.kafkick.api.admin.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.kafkick.api.admin.benchmark.ApiTopologyValidator.MeasuredTopology;
import com.kafkick.api.admin.benchmark.BatchTopologyPreflight.Violation;
import com.kafkick.api.admin.benchmark.dto.BenchmarkCommandAcceptedResponse;
import com.kafkick.api.admin.benchmark.dto.BenchmarkStartRequest;
import com.kafkick.api.caller.Caller;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.benchmark.BenchmarkRunType;
import com.kafkick.core.benchmark.BenchmarkTopology;
import com.kafkick.core.support.exception.CommonErrorCode;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.benchmark.StartBenchmarkRunCommand;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
class BenchmarkStartOrchestratorTest {

    private final ApiTopologyValidator topologyValidator = mock(ApiTopologyValidator.class);
    private final BenchmarkRunService runService = mock(BenchmarkRunService.class);
    private final BenchmarkStartOrchestrator orchestrator =
        new BenchmarkStartOrchestrator(topologyValidator, runService);

    @Test
    @DisplayName("preflight가 실패하면 RUNNING 행을 열지 않는다")
    void failedPreflightStopsBeforeOpen() {
        BenchmarkStartRequest request = request();
        given(topologyValidator.validate(10L, 4, 20_000, 5, 60, 10_000, null, null)).willReturn(new MeasuredTopology(
            topology(), List.of(new Violation("hikari.pool.total", "12", "10", "mismatch"))));

        assertThatThrownBy(() -> orchestrator.start(request, new Caller(7)))
            .isInstanceOf(TopologyValidationException.class)
            .hasMessageContaining("hikari.pool.total");
        verifyNoInteractions(runService);
    }

    @Test
    @DisplayName("실측 preflight가 통과한 뒤에만 완전한 회차 명령으로 RUNNING을 연다")
    void validPreflightOpensRun() {
        BenchmarkStartRequest request = request();
        BenchmarkRun run = mock(BenchmarkRun.class);
        given(run.id()).willReturn(91L);
        given(run.startedAt()).willReturn(Instant.parse("2026-08-23T01:02:03Z"));
        given(topologyValidator.validate(10L, 4, 20_000, 5, 60, 10_000, null, null))
            .willReturn(new MeasuredTopology(topology(), List.of()));
        given(runService.start(org.mockito.ArgumentMatchers.any())).willReturn(run);

        BenchmarkCommandAcceptedResponse response = orchestrator.start(request, new Caller(7));

        ArgumentCaptor<StartBenchmarkRunCommand> command =
            ArgumentCaptor.forClass(StartBenchmarkRunCommand.class);
        verify(runService).start(command.capture());
        assertThat(command.getValue().runKey()).isEqualTo("V3-MAIN-01");
        assertThat(command.getValue().requestedBy()).isEqualTo("7");
        assertThat(command.getValue().topology()).isEqualTo(topology());
        assertThat(command.getValue().loadProfile().offeredRps()).isEqualTo(20_000);
        assertThat(response).isEqualTo(new BenchmarkCommandAcceptedResponse(
            91L, BenchmarkRunState.RUNNING, Instant.parse("2026-08-23T01:02:03Z")));
    }

    @Test
    @DisplayName("회차 서비스 배선 실패는 클라이언트 입력 오류가 아니라 서버 오류다")
    void missingRunServiceIsServerError() {
        BenchmarkStartRequest request = request();
        BenchmarkStartOrchestrator missing = new BenchmarkStartOrchestrator(
            topologyValidator, (BenchmarkRunService) null);
        given(topologyValidator.validate(10L, 4, 20_000, 5, 60, 10_000, null, null))
            .willReturn(new MeasuredTopology(topology(), List.of()));

        assertThatThrownBy(() -> missing.start(request, new Caller(7)))
            .isInstanceOfSatisfying(BusinessException.class, exception ->
                assertThat(exception.getErrorCode()).isEqualTo(CommonErrorCode.INTERNAL_ERROR));
    }

    private static BenchmarkStartRequest request() {
        return new BenchmarkStartRequest(
            "V3-MAIN-01", BenchmarkRunType.MAIN, EngineVersion.V3, ReleaseStage.V3,
            QueueMode.ADAPTIVE, "LOAD_100K", 10L, 4, null, null,
            20_000, 5, 60, 10_000, 0.4, "k6", "1.0", "a".repeat(64));
    }

    private static BenchmarkTopology topology() {
        return new BenchmarkTopology(4, 6, null, null, 60, 16_000, 4_000, 12, 50);
    }
}

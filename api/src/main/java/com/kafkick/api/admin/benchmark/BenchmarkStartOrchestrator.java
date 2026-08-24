package com.kafkick.api.admin.benchmark;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import com.kafkick.api.admin.benchmark.ApiTopologyValidator.MeasuredTopology;
import com.kafkick.api.admin.benchmark.dto.BenchmarkCommandAcceptedResponse;
import com.kafkick.api.admin.benchmark.dto.BenchmarkStartRequest;
import com.kafkick.api.caller.Caller;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.benchmark.LoadProfile;
import com.kafkick.core.benchmark.LoadToolMeta;
import com.kafkick.core.benchmark.StartBenchmarkRunCommand;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/** 토폴로지를 먼저 검증하고 성공한 회차만 RUNNING으로 연다. */
@Service
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class BenchmarkStartOrchestrator {

    private final ApiTopologyValidator topologyValidator;
    private final BenchmarkRunService runService;

    @Autowired
    public BenchmarkStartOrchestrator(
        ApiTopologyValidator topologyValidator,
        ObjectProvider<BenchmarkRunService> runService
    ) {
        this(topologyValidator, runService.getIfAvailable());
    }

    BenchmarkStartOrchestrator(
        ApiTopologyValidator topologyValidator,
        BenchmarkRunService runService
    ) {
        this.topologyValidator = topologyValidator;
        this.runService = runService;
    }

    public BenchmarkCommandAcceptedResponse start(BenchmarkStartRequest request, Caller caller) {
        MeasuredTopology measured = topologyValidator.validate(
            request.couponId(), request.appReplicas(), request.offeredRps(),
            request.loadHoldSeconds(), request.observationHoldSeconds(), request.stockTotal(),
            request.cpuMillicoresTotal(), request.memoryMbTotal());
        if (!measured.valid()) {
            throw new TopologyValidationException(measured.violations());
        }
        BenchmarkRunService service = runService;
        if (service == null) {
            throw new BusinessException(
                CommonErrorCode.INTERNAL_ERROR, "BenchmarkRunService를 사용할 수 없다");
        }
        BenchmarkRun run = service.start(new StartBenchmarkRunCommand(
            request.runKey(),
            request.runType(),
            request.scenarioCode(),
            request.engineVersion(),
            request.releaseStage(),
            request.queueMode(),
            request.couponId(),
            Long.toString(caller.memberId()),
            measured.topology(),
            new LoadProfile(
                request.offeredRps(), request.loadHoldSeconds(), request.observationHoldSeconds(),
                request.stockTotal(), request.generatorIdleRttMillis()),
            new LoadToolMeta(
                request.loadTool(), request.loadToolVersion(), request.loadScriptHash())
        ));
        return new BenchmarkCommandAcceptedResponse(
            run.id(), BenchmarkRunState.RUNNING, run.startedAt());
    }
}

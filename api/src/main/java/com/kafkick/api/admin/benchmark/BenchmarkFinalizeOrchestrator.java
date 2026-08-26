package com.kafkick.api.admin.benchmark;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.api.admin.benchmark.dto.BenchmarkCommandAcceptedResponse;
import com.kafkick.core.admin.BenchmarkRunState;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.benchmark.RunTimeseriesArchiver;
import com.kafkick.core.benchmark.BenchmarkArchiveStatus;
import com.kafkick.core.consistency.ConsistencyFinalStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** 회차 확정이 커밋된 뒤 최초 시계열 archive를 실행한다. */
@Service
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class BenchmarkFinalizeOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkFinalizeOrchestrator.class);

    private final BenchmarkRunService runs;
    private final RunTimeseriesArchiver archiver;
    private final ConsistencyFinalizer consistencyFinalizer;

    @Autowired
    public BenchmarkFinalizeOrchestrator(
        ObjectProvider<BenchmarkRunService> runs,
        ObjectProvider<RunTimeseriesArchiver> archiver,
        ObjectProvider<ConsistencyFinalizer> consistencyFinalizer
    ) {
        this(runs.getIfAvailable(), archiver.getIfAvailable(), consistencyFinalizer.getIfAvailable());
    }

    BenchmarkFinalizeOrchestrator(BenchmarkRunService runs, RunTimeseriesArchiver archiver,
                                  ConsistencyFinalizer consistencyFinalizer) {
        this.runs = runs;
        this.archiver = archiver;
        this.consistencyFinalizer = consistencyFinalizer;
    }

    public BenchmarkCommandAcceptedResponse finalizeRun(long benchmarkRunId) {
        if (runs == null || archiver == null || consistencyFinalizer == null) {
            throw new com.kafkick.core.support.exception.BusinessException(
                com.kafkick.core.support.exception.CommonErrorCode.INTERNAL_ERROR,
                "Benchmark 확정 의존성을 사용할 수 없다");
        }
        BenchmarkRun finalized = runs.finalizeRun(benchmarkRunId);
        BenchmarkArchiveStatus archiveStatus;
        try {
            archiver.archive(benchmarkRunId);
            archiveStatus = BenchmarkArchiveStatus.DONE;
        } catch (RuntimeException failure) {
            log.error("Finalized benchmark archive failed: benchmarkRunId={}", benchmarkRunId, failure);
            try {
                archiveStatus = runs.get(benchmarkRunId).archiveStatus();
            } catch (RuntimeException reloadFailure) {
                reloadFailure.addSuppressed(failure);
                throw reloadFailure;
            }
        }
        ConsistencyFinalStatus consistencyStatus;
        try {
            consistencyFinalizer.calculate(benchmarkRunId);
            consistencyStatus = ConsistencyFinalStatus.DONE;
        } catch (RuntimeException failure) {
            log.error("Finalized benchmark consistency calculation failed: benchmarkRunId={}",
                    benchmarkRunId, failure);
            // 확정과 archive 는 이미 커밋됐다. 상태 재조회 실패로 확정을 오류로 보고하지 않는다.
            try {
                consistencyStatus = runs.get(benchmarkRunId).consistencyStatus();
            } catch (RuntimeException reloadFailure) {
                log.error("Finalized benchmark consistency status reload failed: benchmarkRunId={}",
                        benchmarkRunId, reloadFailure);
                consistencyStatus = null;
            }
        }
        return new BenchmarkCommandAcceptedResponse(
            finalized.id(), BenchmarkRunState.FINALIZED, finalized.finalizedAt(), archiveStatus,
            consistencyStatus);
    }
}

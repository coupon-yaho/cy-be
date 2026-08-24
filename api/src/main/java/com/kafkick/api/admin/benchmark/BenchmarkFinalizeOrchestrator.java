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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/** 회차 확정이 커밋된 뒤 최초 시계열 archive를 실행한다. */
@Service
@ConditionalOnProperty(name = "observation.datasource.enabled", havingValue = "true")
public class BenchmarkFinalizeOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkFinalizeOrchestrator.class);

    private final BenchmarkRunService runs;
    private final RunTimeseriesArchiver archiver;

    @Autowired
    public BenchmarkFinalizeOrchestrator(
        ObjectProvider<BenchmarkRunService> runs,
        ObjectProvider<RunTimeseriesArchiver> archiver
    ) {
        this(runs.getIfAvailable(), archiver.getIfAvailable());
    }

    BenchmarkFinalizeOrchestrator(BenchmarkRunService runs, RunTimeseriesArchiver archiver) {
        this.runs = runs;
        this.archiver = archiver;
    }

    public BenchmarkCommandAcceptedResponse finalizeRun(long benchmarkRunId) {
        if (runs == null || archiver == null) {
            throw new com.kafkick.core.support.exception.BusinessException(
                com.kafkick.api.admin.support.AdminApiErrorCode.NOT_IMPLEMENTED);
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
        return new BenchmarkCommandAcceptedResponse(
            finalized.id(), BenchmarkRunState.FINALIZED, finalized.finalizedAt(), archiveStatus);
    }
}

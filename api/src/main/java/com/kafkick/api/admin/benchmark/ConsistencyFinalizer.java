package com.kafkick.api.admin.benchmark;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.core.benchmark.BenchmarkErrorCode;
import com.kafkick.core.benchmark.BenchmarkRun;
import com.kafkick.core.benchmark.BenchmarkRunService;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyFinalStatus;
import com.kafkick.core.consistency.ConsistencyFinalStore;
import com.kafkick.core.support.exception.BusinessException;

/** batch 계산과 fencing 저장을 조정합니다. 외부 HTTP 중에는 DB 트랜잭션을 잡지 않습니다. */
public class ConsistencyFinalizer {
    private static final Logger log = LoggerFactory.getLogger(ConsistencyFinalizer.class);

    private final BenchmarkRunService runs;
    private final ConsistencyFinalStore store;
    private final BatchConsistencyFinalClient batch;
    private final Duration claimLease;

    public ConsistencyFinalizer(BenchmarkRunService runs, ConsistencyFinalStore store,
                                BatchConsistencyFinalClient batch, Duration claimLease) {
        this.runs = runs;
        this.store = store;
        this.batch = batch;
        this.claimLease = claimLease;
    }

    public void calculate(long benchmarkRunId) {
        if (store == null || batch == null) {
            // 형제 명령인 retryArchive 와 같은 규약이다. 미배선은 500 이 아니라 501 이다.
            throw new BusinessException(
                    com.kafkick.api.admin.support.AdminApiErrorCode.NOT_IMPLEMENTED,
                    "FINAL 정합성 의존성을 사용할 수 없습니다");
        }
        BenchmarkRun run = runs.get(benchmarkRunId);
        if (run.couponId() == null) {
            throw illegalTransition(benchmarkRunId, "couponId is null");
        }
        if (run.finalizedAt() == null) {
            throw illegalTransition(benchmarkRunId, "finalizedAt is null");
        }
        String token = store.claim(benchmarkRunId, claimLease)
                .orElseThrow(() -> illegalTransition(benchmarkRunId,
                        "claim failed: runStatus=" + run.runStatus()
                                + " consistencyStatus=" + run.consistencyStatus()));
        try {
            ConsistencyEvaluation evaluation = batch.evaluate(run.couponId(), run.engineVersion());
            // evaluatedAt은 회차 확정 시각으로 고정한다. 재시도해도 바뀌지 않아야
            // 캠페인별 최신 선택(evaluated_at DESC, run_id DESC)이 흔들리지 않는다.
            if (!store.complete(benchmarkRunId, token, run.couponId(), run.engineVersion(),
                    run.finalizedAt(), evaluation)) {
                throw new IllegalStateException("FINAL 정합성 claim 소유권을 잃었습니다");
            }
        } catch (RuntimeException failure) {
            String message = failure.getMessage();
            String reason = message == null || message.isBlank()
                    ? failure.getClass().getSimpleName() : message;
            try {
                if (!store.fail(benchmarkRunId, token, truncate(reason))) {
                    log.warn("FINAL 정합성 실패 상태를 기록하지 못했습니다: benchmarkRunId={}",
                            benchmarkRunId);
                }
            } catch (RuntimeException statusFailure) {
                failure.addSuppressed(statusFailure);
            }
            throw failure;
        }
    }

    public void retry(long benchmarkRunId) {
        BenchmarkRun run = runs.get(benchmarkRunId);
        if (run.consistencyStatus() == ConsistencyFinalStatus.DONE) {
            throw illegalTransition(benchmarkRunId, "consistencyStatus=DONE");
        }
        calculate(benchmarkRunId);
    }

    /** 컬럼 상한까지 자르되 서로게이트 페어를 반토막 내지 않습니다. */
    private static String truncate(String reason) {
        int end = Math.min(ConsistencyFinalStore.FAILURE_REASON_MAX, reason.length());
        if (end < reason.length() && Character.isHighSurrogate(reason.charAt(end - 1))) {
            end--;
        }
        return reason.substring(0, end);
    }

    private static BusinessException illegalTransition(long id, String actual) {
        return new BusinessException(BenchmarkErrorCode.ILLEGAL_TRANSITION,
                "benchmarkRunId=" + id + " " + actual);
    }
}

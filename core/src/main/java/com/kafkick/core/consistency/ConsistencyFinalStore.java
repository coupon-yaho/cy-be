package com.kafkick.core.consistency;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.observation.EngineVersion;

/** FINAL 정합성 결과와 회차별 claim 상태를 보관하는 포트입니다. */
public interface ConsistencyFinalStore {

    /** benchmark_runs.consistency_failure_reason 컬럼 길이와 같아야 합니다. */
    int FAILURE_REASON_MAX = 200;

    Optional<String> claim(long benchmarkRunId, Duration lease);

    /**
     * 현재 fencing token이 소유한 결과만 저장하고 DONE으로 끝냅니다.
     *
     * @param evaluatedAt 회차 확정 시각. 재시도해도 바뀌지 않는다
     */
    boolean complete(long benchmarkRunId, String claimToken, long couponId,
                     EngineVersion engineVersion, Instant evaluatedAt,
                     ConsistencyEvaluation evaluation);

    /** {@code failureReason}은 {@link #FAILURE_REASON_MAX}자를 넘을 수 없습니다. */
    boolean fail(long benchmarkRunId, String claimToken, String failureReason);

    ConsistencyFinalObservation findLatestByCouponId(long couponId);
}

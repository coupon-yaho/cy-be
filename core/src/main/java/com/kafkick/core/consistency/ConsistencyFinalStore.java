package com.kafkick.core.consistency;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.observation.EngineVersion;

/** FINAL 정합성 결과와 회차별 claim 상태를 보관하는 포트입니다. */
public interface ConsistencyFinalStore {

    /** benchmark_runs.consistency_failure_reason 컬럼 길이와 같아야 합니다. */
    int FAILURE_REASON_MAX = 500;

    /**
     * 소유권을 잡고, 그 claim이 지운 직전 실패 사유를 함께 돌려줍니다. 사유를 따로 읽으면
     * 그 사이 다른 작업자가 남긴 사유를 놓쳐 EXPIRED에 낡은 원인이 실립니다.
     */
    Optional<Claim> claim(long benchmarkRunId, Duration lease);

    /**
     * @param token 이 작업자의 fencing token
     * @param previousFailureReason 이 claim이 지운 직전 실패 사유. 없었으면 {@code null}
     */
    record Claim(String token, String previousFailureReason) {
    }

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

    /**
     * 더 이상 그 회차의 값을 얻을 수 없는 종결 상태로 끝냅니다. FAILED와 달리 다시 claim되지
     * 않으므로 의미 없는 재실행이 원인을 덮어쓰는 일이 생기지 않습니다.
     */
    boolean expire(long benchmarkRunId, String claimToken, String failureReason);

    ConsistencyFinalObservation findLatestByCouponId(long couponId);
}

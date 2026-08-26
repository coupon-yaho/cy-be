package com.kafkick.api.admin.benchmark;

import java.time.Instant;

import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.observation.EngineVersion;

/** batch의 단일 원천 reader로 FINAL 정합성을 계산하는 내부 HTTP 경계입니다. */
public interface BatchConsistencyFinalClient {
    /**
     * @param runFinalizedAt 회차 확정 시각. batch가 허용 지연 창을 넘긴 재실행을 거절하는 기준이다
     */
    ConsistencyEvaluation evaluate(long couponId, EngineVersion engineVersion,
                                   Instant runFinalizedAt);
}

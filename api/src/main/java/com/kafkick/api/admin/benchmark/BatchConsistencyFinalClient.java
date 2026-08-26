package com.kafkick.api.admin.benchmark;

import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.observation.EngineVersion;

/** batch의 단일 원천 reader로 FINAL 정합성을 계산하는 내부 HTTP 경계입니다. */
public interface BatchConsistencyFinalClient {
    ConsistencyEvaluation evaluate(long couponId, EngineVersion engineVersion);
}

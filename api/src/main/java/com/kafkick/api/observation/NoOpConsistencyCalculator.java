package com.kafkick.api.observation;

import com.kafkick.core.consistency.ConsistencyCalculator;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.ConsistencyRawSnapshot;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

import java.util.Map;

public final class NoOpConsistencyCalculator implements ConsistencyCalculator {

    private static final GapValue NOT_APPLICABLE = new GapValue(null, SourceStatus.N_A, null);
    private static final Map<ConsistencyGapType, GapValue> NOT_APPLICABLE_GAPS = Map.of(
            ConsistencyGapType.ACTIVE_DB_GAP, NOT_APPLICABLE,
            ConsistencyGapType.LUA_GAP, NOT_APPLICABLE,
            ConsistencyGapType.PERSIST_GAP, NOT_APPLICABLE,
            ConsistencyGapType.DB_COUNTER_GAP, NOT_APPLICABLE
    );

    @Override
    public ConsistencyEvaluation evaluate(
            ConsistencyRawSnapshot snapshot,
            ConsistencyPhase phase,
            EngineVersion engineVersion
    ) {
        // 계산하지 않은 결과를 FINAL로 표시하면 최종 판정이 완료된 것처럼 보인다.
        return new ConsistencyEvaluation(
                NOT_APPLICABLE_GAPS,
                NOT_APPLICABLE,
                ConsistencyPhase.LIVE,
                null,
                null
        );
    }
}

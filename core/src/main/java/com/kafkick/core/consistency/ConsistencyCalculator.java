package com.kafkick.core.consistency;

import com.kafkick.core.observation.EngineVersion;

public interface ConsistencyCalculator {

    ConsistencyEvaluation evaluate(
            ConsistencyRawSnapshot snapshot,
            ConsistencyPhase phase,
            EngineVersion engineVersion
    );
}

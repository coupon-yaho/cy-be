package com.kafkick.core.consistency;

import java.util.Objects;

public record ConsistencyRawSnapshot(
        ConsistencyRawValues rawValues,
        SourceObservation redisObservation,
        SourceObservation databaseObservation
) {

    public ConsistencyRawSnapshot {
        Objects.requireNonNull(rawValues, "rawValues");
        Objects.requireNonNull(redisObservation, "redisObservation");
        Objects.requireNonNull(databaseObservation, "databaseObservation");
    }
}

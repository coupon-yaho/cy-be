package com.kafkick.core.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;

import java.time.Instant;
import java.util.Objects;

public record RuntimeConfigSnapshot(
        EngineVersion engineVersion,
        ReleaseStage releaseStage,
        QueueMode queueMode,
        long revision,
        Instant updatedAt,
        String updatedBy,
        SourceStatus status
) {

    public RuntimeConfigSnapshot {
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(releaseStage, "releaseStage");
        Objects.requireNonNull(queueMode, "queueMode");
        Objects.requireNonNull(updatedAt, "updatedAt");
        Objects.requireNonNull(status, "status");
        if (revision < 0) {
            throw new IllegalArgumentException("revision은 0 이상이어야 합니다.");
        }
        if (updatedBy == null || updatedBy.isBlank()) {
            throw new IllegalArgumentException("updatedBy는 비어 있을 수 없습니다.");
        }
    }
}

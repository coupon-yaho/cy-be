package com.kafkick.core.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;

import java.util.Objects;

public record RuntimeConfigCommand(
        EngineVersion engineVersion,
        ReleaseStage releaseStage,
        QueueMode queueMode,
        String updatedBy
) {

    public RuntimeConfigCommand {
        Objects.requireNonNull(engineVersion, "engineVersion");
        Objects.requireNonNull(releaseStage, "releaseStage");
        Objects.requireNonNull(queueMode, "queueMode");
        if (updatedBy == null || updatedBy.isBlank()) {
            throw new IllegalArgumentException("updatedBy는 비어 있을 수 없습니다.");
        }
    }
}

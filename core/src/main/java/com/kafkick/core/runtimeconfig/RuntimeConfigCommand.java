package com.kafkick.core.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.exception.BusinessException;

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
            throw new BusinessException(RuntimeConfigErrorCode.INVALID_UPDATED_BY);
        }
    }
}

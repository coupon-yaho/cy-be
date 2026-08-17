package com.kafkick.api.admin.runtimeconfig.dto;

import java.time.Instant;

import com.kafkick.core.admin.EngineVersion;
import com.kafkick.core.admin.QueueMode;
import com.kafkick.core.admin.ReleaseStage;
import com.kafkick.core.admin.SourceStatus;

/** 현재 RuntimeConfig와 원천 상태를 반환합니다. */
public record RuntimeConfigResponse(long revision, EngineVersion engineVersion, ReleaseStage releaseStage,
                                    QueueMode queueMode, Instant updatedAt, Long updatedBy,
                                    SourceStatus sourceStatus) {
}

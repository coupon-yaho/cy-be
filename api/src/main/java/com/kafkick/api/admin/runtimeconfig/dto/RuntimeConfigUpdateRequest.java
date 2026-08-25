package com.kafkick.api.admin.runtimeconfig.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.runtimeconfig.RuntimeConfigCommand;

/**
 * revision 기반 전체 RuntimeConfig 교체 요청입니다.
 *
 * @param expectedRevision 운영자가 조회한 현재 revision
 * @param engineVersion 적용할 엔진 버전
 * @param releaseStage 적용할 릴리스 단계
 * @param queueMode 적용할 대기열 모드
 */
public record RuntimeConfigUpdateRequest(
        @NotNull @PositiveOrZero Long expectedRevision,
        @NotNull EngineVersion engineVersion,
        @NotNull ReleaseStage releaseStage,
        @NotNull QueueMode queueMode) {

    /** HTTP 전체 교체 요청을 Core Runtime Config 갱신 명령으로 변환합니다. */
    public RuntimeConfigCommand toCommand(String updatedBy) {
        return new RuntimeConfigCommand(engineVersion, releaseStage, queueMode, updatedBy);
    }
}

package com.kafkick.api.admin.benchmark.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import com.kafkick.core.admin.EngineVersion;
import com.kafkick.core.admin.QueueMode;
import com.kafkick.core.admin.ReleaseStage;

/** 벤치마크 실행 조건을 고정하는 시작 요청입니다. */
public record BenchmarkStartRequest(
        @NotNull EngineVersion engineVersion,
        @NotNull ReleaseStage releaseStage,
        @NotNull QueueMode queueMode,
        @NotBlank @Size(max = 64)
        @Pattern(regexp = "[A-Z0-9_-]+") String scenarioCode) {
}

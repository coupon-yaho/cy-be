package com.kafkick.api.admin.benchmark.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.Min;

import com.kafkick.core.benchmark.BenchmarkRunType;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;

/** 호출자만 아는 회차 식별·배포 총량·부하 생성기 입력. 서버 토폴로지는 API가 직접 실측한다. */
public record BenchmarkStartRequest(
    @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Z0-9_-]+") String runKey,
    @NotNull BenchmarkRunType runType,
    @NotNull EngineVersion engineVersion,
    @NotNull ReleaseStage releaseStage,
    @NotNull QueueMode queueMode,
    @NotBlank @Size(max = 64) @Pattern(regexp = "[A-Z0-9_-]+") String scenarioCode,
    @NotNull @Positive Long couponId,
    @Min(1) @jakarta.validation.constraints.Max(60) int appReplicas,
    @Positive Integer cpuMillicoresTotal,
    @Positive Integer memoryMbTotal,
    @Positive int offeredRps,
    @Positive int loadHoldSeconds,
    @Positive int observationHoldSeconds,
    @NotNull @PositiveOrZero Integer stockTotal,
    @PositiveOrZero Double generatorIdleRttMillis,
    @Size(max = 32) String loadTool,
    @Size(max = 32) String loadToolVersion,
    @Pattern(regexp = "[0-9a-fA-F]{64}") String loadScriptHash
) {
}

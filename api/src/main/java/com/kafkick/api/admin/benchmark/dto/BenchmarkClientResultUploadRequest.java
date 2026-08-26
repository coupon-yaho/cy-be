package com.kafkick.api.admin.benchmark.dto;

import java.time.Instant;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/** 부하 도구와 무관한 공식 클라이언트 결과 업로드 요청입니다. */
public record BenchmarkClientResultUploadRequest(
        @PositiveOrZero long requestCount, @PositiveOrZero long failureCount,
        @PositiveOrZero long droppedIterations, @PositiveOrZero double tps,
        @PositiveOrZero double p95Millis, @PositiveOrZero double p99Millis,
        @NotNull Instant measuredAt) {
    /** 실패 수가 전체 요청 수를 넘지 않는지 검증합니다. */
    @AssertTrue(message = "failureCount는 requestCount보다 클 수 없습니다.")
    public boolean hasFailureCountWithinRequestCount() { return failureCount <= requestCount; }
}

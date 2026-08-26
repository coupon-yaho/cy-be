package com.kafkick.core.benchmark;

import java.time.Instant;
import java.util.Objects;

/** 시작 시각과 회차 식별자로 이루어진 내림차순 Benchmark Keyset 위치입니다. */
public record BenchmarkRunPosition(Instant startedAt, long benchmarkRunId) {

    /** Cursor를 구성하는 시각과 회차 식별자의 유효성을 검증합니다. */
    public BenchmarkRunPosition {
        Objects.requireNonNull(startedAt, "startedAt");
        if (benchmarkRunId <= 0L) {
            throw new IllegalArgumentException("benchmarkRunId는 양수여야 합니다.");
        }
    }
}

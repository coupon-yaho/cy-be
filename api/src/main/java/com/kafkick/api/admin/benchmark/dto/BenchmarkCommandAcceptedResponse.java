package com.kafkick.api.admin.benchmark.dto;

import java.time.Instant;

import com.kafkick.core.admin.BenchmarkRunState;

/**
 * 비동기 Benchmark 운영 명령이 접수된 직후의 상태를 반환합니다.
 *
 * @param benchmarkRunId 명령 대상 또는 새로 생성된 Benchmark 실행 식별자
 * @param state 명령 접수 후 실행 상태
 * @param requestedAt 서버가 명령을 접수한 시각
 */
public record BenchmarkCommandAcceptedResponse(
        Long benchmarkRunId, BenchmarkRunState state, Instant requestedAt) {
}

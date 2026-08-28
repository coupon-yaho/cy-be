package com.kafkick.api.admin.measurement.dto;

import java.time.Instant;

import com.kafkick.core.admin.MeasurementState;

/**
 * 계측 세션 운영 명령을 적용한 결과입니다.
 *
 * @param benchmarkRunId 계측 대상 Benchmark 실행 식별자
 * @param state 명령 적용 후 계측 세션 상태
 * @param changedAt 상태가 변경된 시각
 */
public record MeasurementSessionResponse(Long benchmarkRunId, MeasurementState state, Instant changedAt) {
}

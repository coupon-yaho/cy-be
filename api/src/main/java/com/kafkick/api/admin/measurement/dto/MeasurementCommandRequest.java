package com.kafkick.api.admin.measurement.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * 계측 세션 시작과 중지 명령에 공통으로 사용하는 요청입니다.
 *
 * @param benchmarkRunId 계측을 시작하거나 중지할 양수 Benchmark 실행 식별자
 */
public record MeasurementCommandRequest(
        @NotNull @Positive Long benchmarkRunId) {
}

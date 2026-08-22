package com.kafkick.api.admin.benchmark.dto;

import java.time.Instant;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.DecimalMax;

/**
 * 서버 Micrometer 값과 구분해 저장할 k6 공식 종료 결과입니다.
 *
 * @param tps k6가 관측한 성공 처리량
 * @param p99Millis k6 종단 관측 p99(ms)
 * @param failureCount timeout·연결 실패를 포함한 실패 건수
 * @param failureRate 전체 요청 대비 실패 비율(0~1)
 * @param measuredAt k6 결과가 확정된 시각
 */
public record K6ResultUploadRequest(
        @NotNull @PositiveOrZero Double tps,
        @NotNull @PositiveOrZero Double p99Millis,
        @NotNull @PositiveOrZero Long failureCount,
        @NotNull @PositiveOrZero @DecimalMax(value = "1.0", message = "failureRate는 1 이하여야 합니다.") Double failureRate,
        @NotNull Instant measuredAt) {
}

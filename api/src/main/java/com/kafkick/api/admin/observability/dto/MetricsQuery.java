package com.kafkick.api.admin.observability.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import com.kafkick.api.admin.support.validation.MutuallyExclusiveMetricsScope;
import com.kafkick.core.admin.MetricsWindow;

/**
 * 관측 지표의 집계 구간과 선택적인 조회 범위를 바인딩하는 변경 가능한 선구축 초안입니다.
 *
 * <p>{@code window}는 {@code 1m}, {@code 5m}, {@code 15m} 중 하나이며 필수입니다. 두 식별자를 모두
 * 생략하면 GLOBAL, {@code couponId}만 있으면 COUPON, {@code benchmarkRunId}만 있으면 BENCHMARK_RUN
 * 범위입니다. 쿠폰과 Benchmark 실행 범위는 동시에 적용할 수 없어 클래스 수준 constraint로 차단합니다.</p>
 *
 * @param window 필수 지표 집계 구간
 * @param couponId 선택 쿠폰 캠페인 회차 범위
 * @param benchmarkRunId 선택 Benchmark 실행 범위
 */
@MutuallyExclusiveMetricsScope
public record MetricsQuery(
        @NotNull(message = "window는 필수입니다.") MetricsWindow window,
        @Positive(message = "couponId는 양수여야 합니다.") Long couponId,
        @Positive(message = "benchmarkRunId는 양수여야 합니다.") Long benchmarkRunId
) { }

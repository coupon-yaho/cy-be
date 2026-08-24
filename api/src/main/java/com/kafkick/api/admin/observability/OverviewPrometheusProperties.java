package com.kafkick.api.admin.observability;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 관리자 Overview의 PromQL 집계 구간과 range 조회 안전 상한을 외부 설정으로 관리합니다. */
@ConfigurationProperties(prefix = "observation.prometheus.overview")
public record OverviewPrometheusProperties(
        Duration currentWindow,
        Duration comparisonOffset,
        Duration trendWindow,
        Duration trendStep,
        Duration outcomeWindow,
        Duration latencyWindow,
        Duration maxRange,
        Integer maxPoints
) {

    private static final Duration DEFAULT_CURRENT_WINDOW = Duration.ofMinutes(1);
    private static final Duration DEFAULT_COMPARISON_OFFSET = Duration.ofMinutes(1);
    private static final Duration DEFAULT_TREND_WINDOW = Duration.ofMinutes(10);
    private static final Duration DEFAULT_TREND_STEP = Duration.ofMinutes(1);
    private static final Duration DEFAULT_OUTCOME_WINDOW = Duration.ofMinutes(5);
    private static final Duration DEFAULT_LATENCY_WINDOW = Duration.ofSeconds(10);
    private static final Duration DEFAULT_MAX_RANGE = Duration.ofHours(1);
    private static final int DEFAULT_MAX_POINTS = 1_000;

    /** 누락된 설정에 기존 동작의 기본값을 적용하고 서로 의존하는 구간·step·상한을 검증합니다. */
    public OverviewPrometheusProperties {
        currentWindow = defaultIfNull(currentWindow, DEFAULT_CURRENT_WINDOW);
        comparisonOffset = defaultIfNull(comparisonOffset, DEFAULT_COMPARISON_OFFSET);
        trendWindow = defaultIfNull(trendWindow, DEFAULT_TREND_WINDOW);
        trendStep = defaultIfNull(trendStep, DEFAULT_TREND_STEP);
        outcomeWindow = defaultIfNull(outcomeWindow, DEFAULT_OUTCOME_WINDOW);
        latencyWindow = defaultIfNull(latencyWindow, DEFAULT_LATENCY_WINDOW);
        maxRange = defaultIfNull(maxRange, DEFAULT_MAX_RANGE);
        maxPoints = maxPoints == null ? DEFAULT_MAX_POINTS : maxPoints;

        requireWholePositiveSeconds(currentWindow, "current-window");
        requireWholePositiveSeconds(comparisonOffset, "comparison-offset");
        requireWholePositiveSeconds(trendWindow, "trend-window");
        requireWholePositiveSeconds(trendStep, "trend-step");
        requireWholePositiveSeconds(outcomeWindow, "outcome-window");
        requireWholePositiveSeconds(latencyWindow, "latency-window");
        requireWholePositiveSeconds(maxRange, "max-range");
        if (maxPoints <= 0) {
            throw new IllegalArgumentException("max-points는 양수여야 합니다.");
        }
        if (trendWindow.compareTo(maxRange) > 0) {
            throw new IllegalArgumentException("trend-window는 max-range를 넘을 수 없습니다.");
        }
        if (trendWindow.toSeconds() % trendStep.toSeconds() != 0L) {
            throw new IllegalArgumentException("trend-window는 trend-step으로 나누어져야 합니다.");
        }
        if (comparisonOffset.compareTo(trendWindow) > 0
                || comparisonOffset.toSeconds() % trendStep.toSeconds() != 0L) {
            // 비교 endpoint가 query_range grid에 없으면 값이 있어도 O1이 PENDING으로 오판됩니다.
            throw new IllegalArgumentException(
                    "comparison-offset은 trend-window 안에서 trend-step 경계와 맞아야 합니다.");
        }
        // query_range는 시작점과 종료점을 모두 포함하므로 화면 버킷 수보다 평가점이 하나 많습니다.
        if (trendWindow.dividedBy(trendStep) + 1L > maxPoints) {
            throw new IllegalArgumentException("추세 조회 평가점 수는 max-points를 넘을 수 없습니다.");
        }
    }

    /** 기존 코드 경로와 단위 테스트가 같은 기본 계약을 재사용하도록 기본 설정을 반환합니다. */
    public static OverviewPrometheusProperties defaults() {
        return new OverviewPrometheusProperties(null, null, null, null, null, null, null, null);
    }

    /** @return 추세 구간을 step으로 나눈 화면 버킷 수 */
    public int expectedTrendBuckets() {
        return Math.toIntExact(trendWindow.dividedBy(trendStep));
    }

    /** PromQL에서 손실 없이 표현할 수 있는 1초 이상의 정수 초 기간만 허용합니다. */
    private static void requireWholePositiveSeconds(Duration value, String name) {
        if (value.isZero() || value.isNegative() || value.getNano() != 0) {
            throw new IllegalArgumentException(name + "은 1초 이상의 정수 초여야 합니다.");
        }
    }

    /** 설정이 없을 때 기존 운영 기본값을 유지합니다. */
    private static Duration defaultIfNull(Duration value, Duration defaultValue) {
        return value == null ? defaultValue : value;
    }
}

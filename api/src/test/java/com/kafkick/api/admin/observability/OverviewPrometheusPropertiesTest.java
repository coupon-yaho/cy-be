package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Overview Prometheus 조회 범위 설정의 불변식을 검증합니다. */
class OverviewPrometheusPropertiesTest {

    @Test
    @DisplayName("추세 구간이 step으로 나누어지지 않으면 설정을 거부한다")
    void rejectsTrendWindowThatCannotBeDividedByStep() {
        assertThatThrownBy(() -> new OverviewPrometheusProperties(
                Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofMinutes(10),
                Duration.ofMinutes(3), Duration.ofMinutes(5), Duration.ofSeconds(10),
                Duration.ofHours(1), 1_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trend-window");
    }

    @Test
    @DisplayName("추세 조회 평가점이 range 상한을 넘으면 설정을 거부한다")
    void rejectsTrendPointsAboveRangeLimit() {
        assertThatThrownBy(() -> new OverviewPrometheusProperties(
                Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofMinutes(10),
                Duration.ofMinutes(1), Duration.ofMinutes(5), Duration.ofSeconds(10),
                Duration.ofHours(1), 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-points");
    }

    @Test
    @DisplayName("비교 offset이 추세 평가점과 맞지 않으면 설정을 거부한다")
    void rejectsComparisonOffsetOutsideTrendGrid() {
        assertThatThrownBy(() -> new OverviewPrometheusProperties(
                Duration.ofMinutes(1), Duration.ofMinutes(3), Duration.ofMinutes(10),
                Duration.ofMinutes(2), Duration.ofMinutes(5), Duration.ofSeconds(10),
                Duration.ofHours(1), 1_000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("comparison-offset");
    }
}

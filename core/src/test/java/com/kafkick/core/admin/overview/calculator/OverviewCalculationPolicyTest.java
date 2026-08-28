package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.OverviewCalculationPolicy;

/** 운영현황 정책의 수치·기간 경계를 검증합니다. */
class OverviewCalculationPolicyTest {

    /** 0·NaN·무한대 비율과 null·0·음수 기간을 정책 값으로 허용하면 안 됩니다. */
    @Test
    void rejectsInvalidRatioAndDurations() {
        assertThatThrownBy(() -> policy(0.0, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy(Double.NaN, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy(Double.POSITIVE_INFINITY, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy(1.0, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy(1.0, Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> policy(1.0, null)).isInstanceOf(NullPointerException.class);
    }

    /** 비율 1은 허용하되 1 초과와 각 기간 필드의 0은 거부합니다. */
    @Test
    void acceptsOneAndValidatesEveryDurationField() {
        new OverviewCalculationPolicy(1.0, Duration.ofSeconds(1), Duration.ofSeconds(1),
                Duration.ofSeconds(1), Duration.ofSeconds(1));
        assertThatThrownBy(() -> new OverviewCalculationPolicy(1.0000001, Duration.ofSeconds(1),
                Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, Duration.ofSeconds(1), Duration.ZERO,
                Duration.ofSeconds(1), Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
    }

    /** 네 정책 기간은 각각 null·0·음수 경계를 독립적으로 거부합니다. */
    @Test
    void validatesEveryDurationFieldIndependently() {
        Duration one = Duration.ofSeconds(1);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, null, one, one, one)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, null, one, one)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, one, null, one)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, one, one, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, Duration.ZERO, one, one, one)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, Duration.ofSeconds(-1), one, one)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, one, Duration.ZERO, one)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, one, Duration.ofSeconds(-1), one)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, one, one, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OverviewCalculationPolicy(0.5, one, one, one, Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
    }

    private static OverviewCalculationPolicy policy(double ratio, Duration duration) {
        return new OverviewCalculationPolicy(ratio, duration, duration, duration, duration);
    }
}

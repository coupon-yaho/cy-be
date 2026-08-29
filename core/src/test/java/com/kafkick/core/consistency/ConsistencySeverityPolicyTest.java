package com.kafkick.core.consistency;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConsistencySeverityPolicyTest {

    @Test
    void providesPrdDefaultThresholds() {
        ConsistencySeverityPolicy policy = ConsistencySeverityPolicy.defaults();

        assertThat(policy.warnThreshold()).isEqualTo(10);
        assertThat(policy.criticalThreshold()).isEqualTo(100);
    }

    @Test
    void rejectsNonPositiveOrNonIncreasingThresholds() {
        assertThatThrownBy(() -> new ConsistencySeverityPolicy(0, 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConsistencySeverityPolicy(10, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ConsistencySeverityPolicy(100, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

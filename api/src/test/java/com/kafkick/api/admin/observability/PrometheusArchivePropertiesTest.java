package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class PrometheusArchivePropertiesTest {

    @Test
    void archiveTimeoutIsIndependentAndLongerThanPollingDefault() {
        PrometheusArchiveProperties defaults = new PrometheusArchiveProperties(null, null);
        PrometheusQueryProperties polling = new PrometheusQueryProperties(null, null, null, null, null);

        assertThat(defaults.readTimeout()).isGreaterThan(polling.readTimeout());
        assertThat(defaults.connectTimeout()).isPositive();
    }

    @Test
    void rejectsNonPositiveTimeouts() {
        assertThatThrownBy(() -> new PrometheusArchiveProperties(Duration.ZERO, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PrometheusArchiveProperties(Duration.ofSeconds(1), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

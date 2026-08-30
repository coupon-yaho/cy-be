package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class QueueGatewayPrometheusPropertiesTest {

    @Test
    void defaultsToDisabledWithFiveSecondFreshness() {
        QueueGatewayPrometheusProperties properties =
                new QueueGatewayPrometheusProperties(null, null);

        assertThat(properties.enabled()).isFalse();
        assertThat(properties.staleAfter()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    void rejectsNonPositiveFreshnessThreshold() {
        assertThatThrownBy(() -> new QueueGatewayPrometheusProperties(true, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueGatewayPrometheusProperties(
                true, Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

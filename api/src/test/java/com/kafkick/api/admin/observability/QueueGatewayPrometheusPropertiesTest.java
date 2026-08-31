package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.nio.file.Files;
import java.nio.file.Path;

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

    @Test
    void observationExampleKeepsTheFeatureDisabledByDefault() throws Exception {
        String yaml = Files.readString(Path.of("src/main/resources/observation.yml.example"));

        assertThat(yaml).contains(
                "enabled: ${QUEUE_GATEWAY_PROMETHEUS_ENABLED:false}",
                "stale-after: ${QUEUE_GATEWAY_PROMETHEUS_STALE_AFTER:5s}");
    }
}

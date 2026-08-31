package com.kafkick.api.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class QueueGatewayPublisherPropertiesTest {

    @Test
    void appliesSafeDisabledDefaults() {
        QueueGatewayPublisherProperties properties = new QueueGatewayPublisherProperties(
                null, null, null, null, null);

        assertThat(properties.enabled()).isFalse();
        assertThat(properties.capacityInterval()).isEqualTo(Duration.ofSeconds(1));
        assertThat(properties.couponRoundInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(properties.creditsPerSecond()).isZero();
        assertThat(properties.instanceId()).isEqualTo("api-local");
    }

    @Test
    void enabledPublisherRequiresBoundedPositiveCapacity() {
        assertThatThrownBy(() -> properties(true, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties(true, 10_001L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(properties(true, 10_000L).creditsPerSecond()).isEqualTo(10_000L);
    }

    @Test
    void rejectsInvalidIntervalsAndInstanceId() {
        assertThatThrownBy(() -> new QueueGatewayPublisherProperties(
                false, Duration.ZERO, Duration.ofSeconds(5), 0L, "api"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueGatewayPublisherProperties(
                false, Duration.ofSeconds(1), Duration.ofSeconds(-1), 0L, "api"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QueueGatewayPublisherProperties(
                false, Duration.ofSeconds(1), Duration.ofSeconds(5), 0L, " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static QueueGatewayPublisherProperties properties(boolean enabled, long capacity) {
        return new QueueGatewayPublisherProperties(
                enabled, Duration.ofSeconds(1), Duration.ofSeconds(5), capacity, "api-1");
    }
}

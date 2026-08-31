package com.kafkick.api.queuegateway;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.scheduling.annotation.Scheduled;

import com.kafkick.core.queuegateway.QueueGatewayStatePort;

class QueueGatewayCapacityPublisherTest {

    private static final Instant NOW = Instant.parse("2026-08-31T01:00:00Z");

    @Test
    void publishesConfiguredSafeCapacityOnlyWhileAcceptingTraffic() {
        QueueGatewayStatePort port = mock(QueueGatewayStatePort.class);
        ApplicationAvailability availability = mock(ApplicationAvailability.class);
        org.mockito.Mockito.when(availability.getReadinessState())
                .thenReturn(ReadinessState.ACCEPTING_TRAFFIC, ReadinessState.REFUSING_TRAFFIC);
        QueueGatewayCapacityPublisher publisher = publisher(port, availability);

        publisher.publishCapacity();
        publisher.publishCapacity();

        verify(port).reportCapacity("api-1", 250L, NOW);
        verify(port).reportCapacity("api-1", 0L, NOW);
    }

    @Test
    void redisFailureDoesNotEscapeTheScheduledBoundaryOrShutdown() {
        QueueGatewayStatePort port = mock(QueueGatewayStatePort.class);
        ApplicationAvailability availability = mock(ApplicationAvailability.class);
        org.mockito.Mockito.when(availability.getReadinessState())
                .thenReturn(ReadinessState.ACCEPTING_TRAFFIC);
        doThrow(new IllegalStateException("redis down"))
                .when(port).reportCapacity("api-1", 250L, NOW);
        doThrow(new IllegalStateException("redis down"))
                .when(port).removeCapacity("api-1");
        QueueGatewayCapacityPublisher publisher = publisher(port, availability);

        publisher.publishCapacity();
        publisher.removeCapacity();
    }

    @Test
    void shutdownRemovesOnlyItsOwnCapacityField() {
        QueueGatewayStatePort port = mock(QueueGatewayStatePort.class);
        QueueGatewayCapacityPublisher publisher = publisher(port, mock(ApplicationAvailability.class));

        publisher.removeCapacity();

        verify(port).removeCapacity("api-1");
    }

    @Test
    void capacityUsesAFixedRateSoRedisCallTimeDoesNotShiftTheOneSecondCadence() throws Exception {
        Scheduled schedule = QueueGatewayCapacityPublisher.class
                .getDeclaredMethod("publishCapacity")
                .getAnnotation(Scheduled.class);

        org.assertj.core.api.Assertions.assertThat(schedule.fixedRateString())
                .isEqualTo("${queue.gateway.publisher.capacity-interval:1s}");
        org.assertj.core.api.Assertions.assertThat(schedule.fixedDelayString()).isEmpty();
        org.assertj.core.api.Assertions.assertThat(schedule.scheduler())
                .isEqualTo(QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER);
    }

    private static QueueGatewayCapacityPublisher publisher(
            QueueGatewayStatePort port, ApplicationAvailability availability) {
        QueueGatewayPublisherProperties properties = new QueueGatewayPublisherProperties(
                true, Duration.ofSeconds(1), Duration.ofSeconds(5), 250L, "api-1");
        return new QueueGatewayCapacityPublisher(
                port, availability, properties, Clock.fixed(NOW, ZoneOffset.UTC));
    }
}

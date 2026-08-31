package com.kafkick.api.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.scheduling.TaskScheduler;

import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.queuegateway.QueueGatewayStatePort;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;

class QueueGatewayPublisherConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(QueueGatewayPublisherConfiguration.class)
            .withBean(QueueGatewayStatePort.class, () -> mock(QueueGatewayStatePort.class))
            .withBean(ApplicationAvailability.class, () -> mock(ApplicationAvailability.class))
            .withBean(AdminCouponRoundDataReader.class, () -> mock(AdminCouponRoundDataReader.class))
            .withBean(RuntimeConfigStore.class, () -> mock(RuntimeConfigStore.class));

    @Test
    void disabledByDefault() {
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(QueueGatewayCapacityPublisher.class);
            assertThat(context).doesNotHaveBean(QueueGatewayCouponRoundPublisher.class);
        });
    }

    @Test
    void enabledConfigurationCreatesBothPublishers() {
        runner.withPropertyValues(
                        "queue.gateway.publisher.enabled=true",
                        "queue.gateway.publisher.credits-per-second=250",
                        "queue.gateway.publisher.instance-id=api-1")
                .run(context -> {
                    assertThat(context).hasSingleBean(QueueGatewayCapacityPublisher.class);
                    assertThat(context).hasSingleBean(QueueGatewayCouponRoundPublisher.class);
                    assertThat(context).hasBean(QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER);
                    assertThat(context).hasBean(QueueGatewayPublisherConfiguration.COUPON_ROUND_SCHEDULER);
                    assertThat(context.getBean(QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER))
                            .isNotSameAs(context.getBean(
                                    QueueGatewayPublisherConfiguration.COUPON_ROUND_SCHEDULER));
                });
    }

    @Test
    void blockedCouponRoundTaskDoesNotDelayCapacityScheduler() {
        runner.withPropertyValues(
                        "queue.gateway.publisher.enabled=true",
                        "queue.gateway.publisher.credits-per-second=250",
                        "queue.gateway.publisher.instance-id=api-1")
                .run(context -> {
                    TaskScheduler capacity = context.getBean(
                            QueueGatewayPublisherConfiguration.CAPACITY_SCHEDULER,
                            TaskScheduler.class);
                    TaskScheduler couponRound = context.getBean(
                            QueueGatewayPublisherConfiguration.COUPON_ROUND_SCHEDULER,
                            TaskScheduler.class);
                    CountDownLatch roundStarted = new CountDownLatch(1);
                    CountDownLatch releaseRound = new CountDownLatch(1);
                    CountDownLatch capacityRan = new CountDownLatch(1);
                    try {
                        couponRound.schedule(() -> {
                            roundStarted.countDown();
                            try {
                                releaseRound.await();
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                            }
                        }, Instant.now());
                        assertThat(roundStarted.await(1, TimeUnit.SECONDS)).isTrue();

                        capacity.schedule(capacityRan::countDown, Instant.now());

                        assertThat(capacityRan.await(1, TimeUnit.SECONDS)).isTrue();
                    } finally {
                        releaseRound.countDown();
                    }
                });
    }

    @Test
    void invalidEnabledCapacityFailsAtStartup() {
        runner.withPropertyValues(
                        "queue.gateway.publisher.enabled=true",
                        "queue.gateway.publisher.credits-per-second=0")
                .run(context -> assertThat(context).hasFailed());
    }
}

package com.kafkick.api.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

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

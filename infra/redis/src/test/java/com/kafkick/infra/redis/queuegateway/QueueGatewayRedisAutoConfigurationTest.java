package com.kafkick.infra.redis.queuegateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.queuegateway.QueueGatewayStatePort;

class QueueGatewayRedisAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    DataRedisAutoConfiguration.class, QueueGatewayRedisAutoConfiguration.class))
            .withBean(StringRedisTemplate.class, () -> mock(StringRedisTemplate.class));

    @Test
    void providesTheGatewayStatePortWhenRedisIsAvailable() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(QueueGatewayStatePort.class);
            assertThat(context.getBean(QueueGatewayStatePort.class))
                    .isInstanceOf(RedisQueueGatewayStateAdapter.class);
        });
    }

    @Test
    void userPortWinsAndConfigurationStepsAsideWithoutRedis() {
        QueueGatewayStatePort userPort = mock(QueueGatewayStatePort.class);
        runner.withBean(QueueGatewayStatePort.class, () -> userPort)
                .run(context -> assertThat(context.getBean(QueueGatewayStatePort.class)).isSameAs(userPort));

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(QueueGatewayRedisAutoConfiguration.class))
                .run(context -> assertThat(context).doesNotHaveBean(QueueGatewayStatePort.class));
    }

    @Test
    void autoConfigurationIsRegisteredInTheBootImportsFile() throws Exception {
        try (var input = getClass().getResourceAsStream(
                "/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports")) {
            assertThat(input).isNotNull();
            assertThat(new String(input.readAllBytes(), StandardCharsets.UTF_8))
                    .contains(QueueGatewayRedisAutoConfiguration.class.getName());
        }
    }
}

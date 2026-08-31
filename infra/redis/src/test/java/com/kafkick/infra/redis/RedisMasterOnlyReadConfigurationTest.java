package com.kafkick.infra.redis;

import io.lettuce.core.ReadFrom;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;

class RedisMasterOnlyReadConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    RedisMasterOnlyReadAutoConfiguration.class,
                    DataRedisAutoConfiguration.class));

    @Test
    void pinsTheSharedRedisConnectionToMasterReads() {
        runner.withPropertyValues(
                        "spring.data.redis.sentinel.master=coupon-master",
                        "spring.data.redis.sentinel.nodes=sentinel-1:26379")
                .run(context -> assertThat(context.getBean(LettuceConnectionFactory.class)
                        .getClientConfiguration().getReadFrom())
                        .contains(ReadFrom.MASTER));
    }

    @Test
    void doesNotChangeStandaloneRedisConnectionConstruction() {
        runner.run(context -> assertThat(context)
                .doesNotHaveBean("redisMasterOnlyReadCustomizer"));
    }
}

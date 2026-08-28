package com.kafkick.api.observation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.infra.redis.lifecycle.CampaignLifecycleRedisAutoConfiguration;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class CampaignLifecycleRedisWiringTest {

    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.2-alpine")
    ).withExposedPorts(6379);

    @BeforeAll
    static void startRedis() {
        REDIS.start();
    }

    @AfterAll
    static void stopRedis() {
        REDIS.stop();
    }

    @Test
    @DisplayName("API 관측 자동설정 뒤에 종료 구독 리스너가 실제 Redis 연결로 기동한다")
    void startSubscriberAfterApiObservationRecorder() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        ApiObservationAutoConfiguration.class,
                        CampaignLifecycleRedisAutoConfiguration.class
                ))
                .withPropertyValues(
                        "campaign.lifecycle.redis.subscriber-enabled=true"
                )
                .withBean(ObjectMapper.class, () -> JsonMapper.builder()
                        .findAndAddModules()
                        .build())
                .withBean(SimpleMeterRegistry.class,
                        SimpleMeterRegistry::new)
                .withBean(LettuceConnectionFactory.class, () ->
                        new LettuceConnectionFactory(
                                REDIS.getHost(),
                                REDIS.getMappedPort(6379)
                        ))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(
                            RedisMessageListenerContainer.class
                    );
                    assertThat(context.getBean(
                            RedisMessageListenerContainer.class
                    ).isRunning()).isTrue();
                });
    }
}

package com.kafkick.infra.redis.lifecycle;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class RecoveringRedisMessageListenerContainerTest {

    @Test
    @DisplayName("stop 이후 동일한 Redis 구독 컨테이너를 다시 시작할 수 있다")
    void restartAfterStop() throws Exception {
        RecoveringRedisMessageListenerContainer container =
                new RecoveringRedisMessageListenerContainer();
        container.setConnectionFactory(mock(RedisConnectionFactory.class));
        container.afterPropertiesSet();
        try {
            container.start();
            assertThat(container.isRunning()).isTrue();

            container.stop();
            assertThat(container.isRunning()).isFalse();

            container.start();

            assertThat(container.isRunning()).isTrue();
        } finally {
            container.destroy();
        }
    }
}

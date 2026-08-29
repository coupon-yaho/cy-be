package com.kafkick.infra.redis.lifecycle;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.kafkick.core.observation.CampaignClosedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class CampaignLifecycleRedisIntegrationTest {

    private static final String CHANNEL =
            "campaign:lifecycle:closed:integration";
    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder().findAndAddModules().build();

    private static GenericContainer<?> redis;
    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redisTemplate;
    private static RedisMessageListenerContainer firstContainer;
    private static RedisMessageListenerContainer secondContainer;
    private static List<Long> firstReceived;
    private static List<Long> secondReceived;

    @BeforeAll
    static void startRedisAndSubscribers() {
        redis = new GenericContainer<>(
                DockerImageName.parse("redis:7.2-alpine")
        ).withExposedPorts(6379);
        redis.start();
        connectionFactory = new LettuceConnectionFactory(
                redis.getHost(),
                redis.getMappedPort(6379)
        );
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.afterPropertiesSet();
        firstReceived = new CopyOnWriteArrayList<>();
        secondReceived = new CopyOnWriteArrayList<>();
        firstContainer = subscriber(firstReceived);
        secondContainer = subscriber(secondReceived);
        firstContainer.start();
        secondContainer.start();
    }

    @AfterAll
    static void stopRedisAndSubscribers() throws Exception {
        if (firstContainer != null) {
            firstContainer.destroy();
        }
        if (secondContainer != null) {
            secondContainer.destroy();
        }
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
        if (redis != null) {
            redis.stop();
        }
    }

    @Test
    @DisplayName("모든 API가 수신하고 단절 중 메시지는 유실되며 재연결 뒤 신규 메시지는 받는다")
    void fanOutLossAndReconnect() {
        RedisCampaignClosedEventPublisher publisher =
                new RedisCampaignClosedEventPublisher(
                        redisTemplate,
                        OBJECT_MAPPER,
                        CHANNEL
                );

        publisher.publish(event(201L));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(firstReceived).containsExactly(201L);
            assertThat(secondReceived).containsExactly(201L);
        });

        secondContainer.stop();
        publisher.publish(event(202L));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(firstReceived).containsExactly(201L, 202L));
        assertThat(secondReceived).containsExactly(201L);

        secondContainer.start();
        publisher.publish(event(203L));

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(firstReceived).containsExactly(201L, 202L, 203L);
            assertThat(secondReceived).containsExactly(201L, 203L);
        });
    }

    @Test
    @DisplayName("Redis가 꺼진 채 기동해도 API는 뜨고 Redis 복구 뒤 구독을 시작한다")
    void recoverSubscriptionAfterInitialConnectionFailure() throws Exception {
        GenericContainer<?> recoveringRedis = new GenericContainer<>(
                DockerImageName.parse("redis:7.2-alpine")
        ).withExposedPorts(6379);
        recoveringRedis.start();
        LettuceConnectionFactory recoveringConnection =
                new LettuceConnectionFactory(
                        recoveringRedis.getHost(),
                        recoveringRedis.getMappedPort(6379)
                );
        recoveringConnection.afterPropertiesSet();
        recoveringConnection.start();
        StringRedisTemplate recoveringTemplate =
                new StringRedisTemplate(recoveringConnection);
        recoveringTemplate.afterPropertiesSet();
        List<Long> recovered = new CopyOnWriteArrayList<>();
        RecoveringRedisMessageListenerContainer recoveringContainer =
                new RecoveringRedisMessageListenerContainer(100L);
        recoveringContainer.setConnectionFactory(recoveringConnection);
        recoveringContainer.addMessageListener(
                new RedisCampaignClosedEventSubscriber(
                        OBJECT_MAPPER,
                        (campaignCouponId, closedAt) ->
                                recovered.add(campaignCouponId)
                ),
                new ChannelTopic(CHANNEL)
        );
        recoveringContainer.afterPropertiesSet();
        try {
            recoveringRedis.getDockerClient()
                    .pauseContainerCmd(recoveringRedis.getContainerId())
                    .exec();

            recoveringContainer.start();

            assertThat(recoveringContainer.isRunning()).isFalse();
            assertThat(recoveringContainer.isListening()).isFalse();

            recoveringRedis.getDockerClient()
                    .unpauseContainerCmd(recoveringRedis.getContainerId())
                    .exec();
            await().ignoreExceptions()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(
                            recoveringConnection.getConnection().ping()
                    ).isEqualTo("PONG"));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(recoveringContainer.isListening()).isTrue());

            new RedisCampaignClosedEventPublisher(
                    recoveringTemplate,
                    OBJECT_MAPPER,
                    CHANNEL
            ).publish(event(301L));

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(recovered).containsExactly(301L));
        } finally {
            recoveringContainer.destroy();
            recoveringConnection.destroy();
            recoveringRedis.stop();
        }
    }

    private static RedisMessageListenerContainer subscriber(
            List<Long> received
    ) {
        RedisCampaignClosedEventSubscriber subscriber =
                new RedisCampaignClosedEventSubscriber(
                        OBJECT_MAPPER,
                        (campaignCouponId, closedAt) ->
                                received.add(campaignCouponId)
                );
        RedisMessageListenerContainer container =
                new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                subscriber,
                new ChannelTopic(CHANNEL)
        );
        container.afterPropertiesSet();
        return container;
    }

    private static CampaignClosedEvent event(long campaignCouponId) {
        return new CampaignClosedEvent(
                campaignCouponId,
                Instant.parse("2026-08-26T05:04:00Z")
        );
    }
}

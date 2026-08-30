package com.kafkick.infra.redis.lifecycle;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import com.kafkick.core.observation.CouponRoundClosedEvent;
import com.kafkick.core.observation.CouponRoundLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CouponRoundLifecycleRedisAutoConfigurationTest {

    private static final CouponRoundClosedEvent EVENT = new CouponRoundClosedEvent(
            201L,
            Instant.parse("2026-08-26T05:04:00Z")
    );

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(
                            CouponRoundLifecycleRedisAutoConfiguration.class
                    ))
                    .withBean(
                            ObjectMapper.class,
                            () -> JsonMapper.builder()
                                    .findAndAddModules()
                                    .build()
                    );

    @Test
    @DisplayName("기본 쿠폰 회차 종료 채널 Publisher를 자동 설정한다")
    void configurePublisherWithDefaultChannel() {
        contextRunner.withBean(
                StringRedisTemplate.class,
                () -> mock(StringRedisTemplate.class)
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(
                    RedisCouponRoundClosedEventPublisher.class
            );
            RedisCouponRoundClosedEventPublisher publisher = context.getBean(
                    RedisCouponRoundClosedEventPublisher.class
            );
            StringRedisTemplate redis = context.getBean(
                    StringRedisTemplate.class
            );

            publisher.publish(EVENT);

            verify(redis).convertAndSend(
                    eq("coupon-round:lifecycle:closed"),
                    anyString()
            );
        });
    }

    @Test
    @DisplayName("쿠폰 회차 종료 Redis 채널을 환경별로 바꿀 수 있다")
    void configurePublisherWithOverriddenChannel() {
        contextRunner.withPropertyValues(
                "coupon-round.lifecycle.redis.channel=staging:coupon-round:closed"
        ).withBean(
                StringRedisTemplate.class,
                () -> mock(StringRedisTemplate.class)
        ).run(context -> {
            RedisCouponRoundClosedEventPublisher publisher = context.getBean(
                    RedisCouponRoundClosedEventPublisher.class
            );
            StringRedisTemplate redis = context.getBean(
                    StringRedisTemplate.class
            );

            publisher.publish(EVENT);

            verify(redis).convertAndSend(
                    eq("staging:coupon-round:closed"),
                    anyString()
            );
        });
    }

    @Test
    @DisplayName("Redis 템플릿이 없으면 Publisher를 만들지 않는다")
    void doNotConfigurePublisherWithoutRedisTemplate() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(
                    RedisCouponRoundClosedEventPublisher.class
            );
        });
    }

    @Test
    @DisplayName("사용자 정의 Publisher가 있으면 자동 설정이 물러난다")
    void backOffForCustomPublisher() {
        RedisCouponRoundClosedEventPublisher custom = mock(
                RedisCouponRoundClosedEventPublisher.class
        );
        contextRunner.withBean(
                StringRedisTemplate.class,
                () -> mock(StringRedisTemplate.class)
        ).withBean(
                RedisCouponRoundClosedEventPublisher.class,
                () -> custom
        ).run(context -> {
            assertThat(context).hasSingleBean(
                    RedisCouponRoundClosedEventPublisher.class
            );
            assertThat(context.getBean(
                    RedisCouponRoundClosedEventPublisher.class
            )).isSameAs(custom);
        });
    }

    @Test
    @DisplayName("구독은 기본값으로 꺼져 있어 발행 JVM이 자기 메시지를 받지 않는다")
    void subscriberIsDisabledByDefault() {
        contextRunner.withBean(
                RedisConnectionFactory.class,
                () -> mock(RedisConnectionFactory.class)
        ).withBean(
                CouponRoundLifecycleRecorder.class,
                () -> mock(CouponRoundLifecycleRecorder.class)
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(
                    RedisMessageListenerContainer.class
            );
        });
    }

    @Test
    @DisplayName("구독을 켜도 미터 회수 포트가 없는 JVM에는 리스너를 만들지 않는다")
    void subscriberRequiresCouponRoundLifecycleRecorder() {
        contextRunner.withPropertyValues(
                "coupon-round.lifecycle.redis.subscriber-enabled=true"
        ).withBean(
                RedisConnectionFactory.class,
                () -> mock(RedisConnectionFactory.class)
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(
                    RedisMessageListenerContainer.class
            );
        });
    }

    @Test
    @DisplayName("Redis 초기 연결 실패가 API 컨텍스트 기동을 막지 않는다")
    void isolateSubscriberInitialConnectionFailure() {
        RedisConnectionFactory connectionFactory =
                mock(RedisConnectionFactory.class);
        when(connectionFactory.getConnection()).thenThrow(
                new RedisConnectionFailureException("연결 거부")
        );

        contextRunner.withPropertyValues(
                "coupon-round.lifecycle.redis.subscriber-enabled=true"
        ).withBean(
                RedisConnectionFactory.class,
                () -> connectionFactory
        ).withBean(
                CouponRoundLifecycleRecorder.class,
                () -> mock(CouponRoundLifecycleRecorder.class)
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(
                    RedisMessageListenerContainer.class
            );
            assertThat(context.getBean(
                    RedisMessageListenerContainer.class
            )).isInstanceOf(
                    RecoveringRedisMessageListenerContainer.class
            );
        });
    }

    @Test
    @DisplayName("Redis 연결 실패가 아닌 구독 설정 오류는 컨텍스트 기동을 실패시킨다")
    void propagateNonConnectionSubscriberFailure() {
        RedisConnectionFactory connectionFactory =
                mock(RedisConnectionFactory.class);
        when(connectionFactory.getConnection()).thenThrow(
                new IllegalArgumentException("잘못된 연결 설정")
        );

        contextRunner.withPropertyValues(
                "coupon-round.lifecycle.redis.subscriber-enabled=true"
        ).withBean(
                RedisConnectionFactory.class,
                () -> connectionFactory
        ).withBean(
                CouponRoundLifecycleRecorder.class,
                () -> mock(CouponRoundLifecycleRecorder.class)
        ).run(context -> assertThat(context).hasFailed());
    }
}

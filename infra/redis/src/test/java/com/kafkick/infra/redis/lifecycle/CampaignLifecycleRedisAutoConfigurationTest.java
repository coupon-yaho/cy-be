package com.kafkick.infra.redis.lifecycle;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.observation.CampaignClosedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CampaignLifecycleRedisAutoConfigurationTest {

    private static final CampaignClosedEvent EVENT = new CampaignClosedEvent(
            201L,
            Instant.parse("2026-08-26T05:04:00Z")
    );

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(
                            CampaignLifecycleRedisAutoConfiguration.class
                    ))
                    .withBean(
                            ObjectMapper.class,
                            () -> JsonMapper.builder()
                                    .findAndAddModules()
                                    .build()
                    );

    @Test
    @DisplayName("기본 캠페인 종료 채널 Publisher를 자동 설정한다")
    void configurePublisherWithDefaultChannel() {
        contextRunner.withBean(
                StringRedisTemplate.class,
                () -> mock(StringRedisTemplate.class)
        ).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(
                    RedisCampaignClosedEventPublisher.class
            );
            RedisCampaignClosedEventPublisher publisher = context.getBean(
                    RedisCampaignClosedEventPublisher.class
            );
            StringRedisTemplate redis = context.getBean(
                    StringRedisTemplate.class
            );

            publisher.publish(EVENT);

            verify(redis).convertAndSend(
                    eq("campaign:lifecycle:closed"),
                    anyString()
            );
        });
    }

    @Test
    @DisplayName("캠페인 종료 Redis 채널을 환경별로 바꿀 수 있다")
    void configurePublisherWithOverriddenChannel() {
        contextRunner.withPropertyValues(
                "campaign.lifecycle.redis.channel=staging:campaign:closed"
        ).withBean(
                StringRedisTemplate.class,
                () -> mock(StringRedisTemplate.class)
        ).run(context -> {
            RedisCampaignClosedEventPublisher publisher = context.getBean(
                    RedisCampaignClosedEventPublisher.class
            );
            StringRedisTemplate redis = context.getBean(
                    StringRedisTemplate.class
            );

            publisher.publish(EVENT);

            verify(redis).convertAndSend(
                    eq("staging:campaign:closed"),
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
                    RedisCampaignClosedEventPublisher.class
            );
        });
    }

    @Test
    @DisplayName("사용자 정의 Publisher가 있으면 자동 설정이 물러난다")
    void backOffForCustomPublisher() {
        RedisCampaignClosedEventPublisher custom = mock(
                RedisCampaignClosedEventPublisher.class
        );
        contextRunner.withBean(
                StringRedisTemplate.class,
                () -> mock(StringRedisTemplate.class)
        ).withBean(
                RedisCampaignClosedEventPublisher.class,
                () -> custom
        ).run(context -> {
            assertThat(context).hasSingleBean(
                    RedisCampaignClosedEventPublisher.class
            );
            assertThat(context.getBean(
                    RedisCampaignClosedEventPublisher.class
            )).isSameAs(custom);
        });
    }
}

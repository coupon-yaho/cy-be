package com.kafkick.infra.redis.lifecycle;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.kafkick.core.observation.CampaignClosedEvent;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RedisCampaignClosedEventPublisherTest {

    private static final String CHANNEL = "campaign:lifecycle:closed:test";
    private static final CampaignClosedEvent EVENT = new CampaignClosedEvent(
            201L,
            Instant.parse("2026-08-26T05:04:00Z")
    );

    private final ObjectMapper objectMapper =
            JsonMapper.builder().findAndAddModules().build();

    @Test
    @DisplayName("종료 이벤트를 Jackson 3 JSON으로 직렬화해 설정 채널로 발행한다")
    void publishJsonToConfiguredChannel() throws Exception {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RedisCampaignClosedEventPublisher publisher =
                new RedisCampaignClosedEventPublisher(
                        redis,
                        objectMapper,
                        CHANNEL
                );
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);

        publisher.publish(EVENT);

        verify(redis).convertAndSend(eq(CHANNEL), payload.capture());
        assertThat(objectMapper.readValue(
                payload.getValue(),
                CampaignClosedEvent.class
        )).isEqualTo(EVENT);
    }

    @Test
    @DisplayName("직렬화 실패를 삼키고 Redis를 호출하지 않는다")
    void isolateSerializationFailure() throws Exception {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        when(failingMapper.writeValueAsString(any()))
                .thenThrow(mock(tools.jackson.core.JacksonException.class));
        RedisCampaignClosedEventPublisher publisher =
                new RedisCampaignClosedEventPublisher(
                        redis,
                        failingMapper,
                        CHANNEL
                );

        assertThatCode(() -> publisher.publish(EVENT))
                .doesNotThrowAnyException();
        verifyNoInteractions(redis);
    }

    @Test
    @DisplayName("Redis 발행 실패를 삼켜 이미 커밋된 Lifecycle 성공을 유지한다")
    void isolateRedisFailure() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.convertAndSend(eq(CHANNEL), any(String.class)))
                .thenThrow(new RedisConnectionFailureException("연결 끊김"));
        RedisCampaignClosedEventPublisher publisher =
                new RedisCampaignClosedEventPublisher(
                        redis,
                        objectMapper,
                        CHANNEL
                );

        assertThatCode(() -> publisher.publish(EVENT))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("연속 Redis 발행 실패를 제한된 WARN 한 줄로 요약한다")
    void throttleRepeatedFailureWarnings() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.convertAndSend(eq(CHANNEL), any(String.class)))
                .thenThrow(new RedisConnectionFailureException("연결 끊김"));
        RedisCampaignClosedEventPublisher publisher =
                new RedisCampaignClosedEventPublisher(
                        redis,
                        objectMapper,
                        CHANNEL
                );
        Logger logger = (Logger) LoggerFactory.getLogger(
                RedisCampaignClosedEventPublisher.class
        );
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        try {
            publisher.publish(EVENT);
            publisher.publish(EVENT);
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }

        assertThat(logs.list).hasSize(1);
        assertThat(logs.list.getFirst().getFormattedMessage())
                .contains("누적 1건");
    }
}

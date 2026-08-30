package com.kafkick.infra.redis.lifecycle;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.kafkick.core.observation.CouponRoundClosedEvent;
import com.kafkick.core.observation.CouponRoundLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class CouponRoundClosedEventJsonContractTest {

    @Test
    void publisherPayloadIsConsumedByTheSubscriberWithoutTranslation() {
        String channel = "coupon-round:lifecycle:closed:test";
        Instant closedAt = Instant.parse("2026-08-26T05:04:00Z");
        ObjectMapper objectMapper = JsonMapper.builder().findAndAddModules().build();
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);

        new RedisCouponRoundClosedEventPublisher(redis, objectMapper, channel)
                .publish(new CouponRoundClosedEvent(201L, closedAt));
        verify(redis).convertAndSend(eq(channel), payload.capture());

        CouponRoundLifecycleRecorder recorder = mock(CouponRoundLifecycleRecorder.class);
        new RedisCouponRoundClosedEventSubscriber(objectMapper, recorder).onMessage(
                new DefaultMessage(
                        channel.getBytes(StandardCharsets.UTF_8),
                        payload.getValue().getBytes(StandardCharsets.UTF_8)),
                null);

        verify(recorder).retireCouponRound(201L, closedAt);
    }
}

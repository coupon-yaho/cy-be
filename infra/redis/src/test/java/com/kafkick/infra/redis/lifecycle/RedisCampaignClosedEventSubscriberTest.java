package com.kafkick.infra.redis.lifecycle;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.DefaultMessage;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import com.kafkick.core.observation.CampaignLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class RedisCampaignClosedEventSubscriberTest {

    private static final Instant CLOSED_AT =
            Instant.parse("2026-08-26T05:04:00Z");

    private final ObjectMapper objectMapper =
            JsonMapper.builder().findAndAddModules().build();

    @Test
    @DisplayName("종료 JSON을 검증해 캠페인 미터 회수 포트로 전달한다")
    void retireCampaignFromValidJson() {
        CampaignLifecycleRecorder recorder = mock(CampaignLifecycleRecorder.class);
        RedisCampaignClosedEventSubscriber subscriber = subscriber(recorder);

        subscriber.onMessage(message("""
                {"campaignCouponId":201,"closedAt":"2026-08-26T05:04:00Z"}
                """), null);

        verify(recorder).retireCampaign(201L, CLOSED_AT);
    }

    @Test
    @DisplayName("중복 종료 메시지도 포트에 전달해 기존 미터 회수 멱등성에 맡긴다")
    void delegateDuplicateMessagesWithoutSubscriberDeduplication() {
        CampaignLifecycleRecorder recorder = mock(CampaignLifecycleRecorder.class);
        RedisCampaignClosedEventSubscriber subscriber = subscriber(recorder);
        DefaultMessage message = message("""
                {"campaignCouponId":201,"closedAt":"2026-08-26T05:04:00Z"}
                """);

        subscriber.onMessage(message, null);
        subscriber.onMessage(message, null);

        verify(recorder, times(2)).retireCampaign(201L, CLOSED_AT);
    }

    @Test
    @DisplayName("깨진 JSON과 값 계약 위반 메시지는 버리고 리스너 스레드를 유지한다")
    void isolateMalformedAndInvalidMessages() {
        CampaignLifecycleRecorder recorder = mock(CampaignLifecycleRecorder.class);
        RedisCampaignClosedEventSubscriber subscriber = subscriber(recorder);

        assertThatCode(() -> subscriber.onMessage(message("not-json"), null))
                .doesNotThrowAnyException();
        assertThatCode(() -> subscriber.onMessage(message("""
                {"campaignCouponId":0,"closedAt":"2026-08-26T05:04:00Z"}
                """), null)).doesNotThrowAnyException();
        assertThatCode(() -> subscriber.onMessage(message("""
                {"campaignCouponId":201}
                """), null)).doesNotThrowAnyException();

        verifyNoInteractions(recorder);
    }

    @Test
    @DisplayName("미터 회수 실패를 삼켜 다음 Redis 메시지를 받을 수 있게 한다")
    void isolateRecorderFailure() {
        CampaignLifecycleRecorder recorder = mock(CampaignLifecycleRecorder.class);
        doThrow(new IllegalStateException("meter failure"))
                .when(recorder).retireCampaign(201L, CLOSED_AT);
        RedisCampaignClosedEventSubscriber subscriber = subscriber(recorder);

        assertThatCode(() -> subscriber.onMessage(message("""
                {"campaignCouponId":201,"closedAt":"2026-08-26T05:04:00Z"}
                """), null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("연속 오염 메시지를 제한된 WARN 한 줄로 요약한다")
    void throttleRepeatedFailureWarnings() {
        RedisCampaignClosedEventSubscriber subscriber = subscriber(
                mock(CampaignLifecycleRecorder.class)
        );
        Logger logger = (Logger) LoggerFactory.getLogger(
                RedisCampaignClosedEventSubscriber.class
        );
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        try {
            subscriber.onMessage(message("not-json"), null);
            subscriber.onMessage(message("still-not-json"), null);
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }

        assertThat(logs.list).hasSize(1);
        assertThat(logs.list.getFirst().getFormattedMessage())
                .contains("누적 1건");
    }

    private RedisCampaignClosedEventSubscriber subscriber(
            CampaignLifecycleRecorder recorder
    ) {
        return new RedisCampaignClosedEventSubscriber(objectMapper, recorder);
    }

    private static DefaultMessage message(String body) {
        return new DefaultMessage(
                "campaign:lifecycle:closed".getBytes(StandardCharsets.UTF_8),
                body.getBytes(StandardCharsets.UTF_8)
        );
    }
}

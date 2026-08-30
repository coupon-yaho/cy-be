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

import com.kafkick.core.observation.CouponRoundLifecycleRecorder;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class RedisCouponRoundClosedEventSubscriberTest {

    private static final Instant CLOSED_AT =
            Instant.parse("2026-08-26T05:04:00Z");

    private final ObjectMapper objectMapper =
            JsonMapper.builder().findAndAddModules().build();

    @Test
    @DisplayName("종료 JSON을 검증해 쿠폰 회차 미터 회수 포트로 전달한다")
    void retireCouponRoundFromValidJson() {
        CouponRoundLifecycleRecorder recorder = mock(CouponRoundLifecycleRecorder.class);
        RedisCouponRoundClosedEventSubscriber subscriber = subscriber(recorder);

        subscriber.onMessage(message("""
                {"couponId":201,"closedAt":"2026-08-26T05:04:00Z"}
                """), null);

        verify(recorder).retireCouponRound(201L, CLOSED_AT);
    }

    @Test
    @DisplayName("중복 종료 메시지도 포트에 전달해 기존 미터 회수 멱등성에 맡긴다")
    void delegateDuplicateMessagesWithoutSubscriberDeduplication() {
        CouponRoundLifecycleRecorder recorder = mock(CouponRoundLifecycleRecorder.class);
        RedisCouponRoundClosedEventSubscriber subscriber = subscriber(recorder);
        DefaultMessage message = message("""
                {"couponId":201,"closedAt":"2026-08-26T05:04:00Z"}
                """);

        subscriber.onMessage(message, null);
        subscriber.onMessage(message, null);

        verify(recorder, times(2)).retireCouponRound(201L, CLOSED_AT);
    }

    @Test
    @DisplayName("깨진 JSON과 값 계약 위반 메시지는 버리고 리스너 스레드를 유지한다")
    void isolateMalformedAndInvalidMessages() {
        CouponRoundLifecycleRecorder recorder = mock(CouponRoundLifecycleRecorder.class);
        RedisCouponRoundClosedEventSubscriber subscriber = subscriber(recorder);

        assertThatCode(() -> subscriber.onMessage(message("not-json"), null))
                .doesNotThrowAnyException();
        assertThatCode(() -> subscriber.onMessage(message("""
                {"couponId":0,"closedAt":"2026-08-26T05:04:00Z"}
                """), null)).doesNotThrowAnyException();
        assertThatCode(() -> subscriber.onMessage(message("""
                {"couponId":201}
                """), null)).doesNotThrowAnyException();

        verifyNoInteractions(recorder);
    }

    @Test
    @DisplayName("미터 회수 실패를 삼켜 다음 Redis 메시지를 받을 수 있게 한다")
    void isolateRecorderFailure() {
        CouponRoundLifecycleRecorder recorder = mock(CouponRoundLifecycleRecorder.class);
        doThrow(new IllegalStateException("meter failure"))
                .when(recorder).retireCouponRound(201L, CLOSED_AT);
        RedisCouponRoundClosedEventSubscriber subscriber = subscriber(recorder);

        assertThatCode(() -> subscriber.onMessage(message("""
                {"couponId":201,"closedAt":"2026-08-26T05:04:00Z"}
                """), null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("연속 오염 메시지를 제한된 WARN 한 줄로 요약한다")
    void throttleRepeatedFailureWarnings() {
        RedisCouponRoundClosedEventSubscriber subscriber = subscriber(
                mock(CouponRoundLifecycleRecorder.class)
        );
        Logger logger = (Logger) LoggerFactory.getLogger(
                RedisCouponRoundClosedEventSubscriber.class
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

    private RedisCouponRoundClosedEventSubscriber subscriber(
            CouponRoundLifecycleRecorder recorder
    ) {
        return new RedisCouponRoundClosedEventSubscriber(objectMapper, recorder);
    }

    private static DefaultMessage message(String body) {
        return new DefaultMessage(
                "coupon-round:lifecycle:closed".getBytes(StandardCharsets.UTF_8),
                body.getBytes(StandardCharsets.UTF_8)
        );
    }
}

package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import com.kafkick.core.notification.FailureOutcome;
import com.kafkick.core.notification.NotificationDeliveryDecision;
import com.kafkick.core.notification.NotificationDeliveryService;
import com.kafkick.core.notification.NotificationSendException;
import com.kafkick.core.notification.NotificationSender;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.infra.mq.config.KafkaTopicConfig;

@ExtendWith(MockitoExtension.class)
class NotificationDispatchConsumerTest {
    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");

    @Mock NotificationDeliveryService deliveries;
    @Mock NotificationSender sender;
    @Mock NotificationResultMeter meter;
    @Mock Acknowledgment acknowledgment;
    private NotificationDispatchConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new NotificationDispatchConsumer(deliveries, sender, meter,
                Clock.fixed(AT, ZoneOffset.UTC));
    }

    @Test
    void successfulWinnerSendsOnceMetersOnceAndAcknowledges() {
        NotificationDeliveryDecision decision = sendDecision(1);
        when(deliveries.prepare(event(), AT)).thenReturn(decision);
        when(deliveries.completeSuccess(decision, AT, AT)).thenReturn(true);

        consumer.consume(record(), acknowledgment);

        verify(sender).send(decision.notification());
        verify(meter).success();
        verify(acknowledgment).acknowledge();
    }

    @Test
    void duplicateDecisionDoesNotSendAndAcknowledges() {
        when(deliveries.prepare(event(), AT))
                .thenReturn(NotificationDeliveryDecision.acknowledge());

        consumer.consume(record(), acknowledgment);

        verify(sender, never()).send(org.mockito.ArgumentMatchers.any());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void retryableFailureIsPersistedThenThrownForKafkaBackoff() {
        NotificationDeliveryDecision decision = sendDecision(1);
        when(deliveries.prepare(event(), AT)).thenReturn(decision);
        org.mockito.Mockito.doThrow(new NotificationSendException(
                NotifyFailureReason.SEND_TIMEOUT, new RuntimeException("timeout")))
                .when(sender).send(decision.notification());
        when(deliveries.completeFailure(decision, NotifyFailureReason.SEND_TIMEOUT, AT, AT))
                .thenReturn(FailureOutcome.RETRY);

        assertThatThrownBy(() -> consumer.consume(record(), acknowledgment))
                .isInstanceOf(NotificationRetryableException.class);

        verify(meter, never()).failure();
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void terminalFailureWinnerMetersOnceAndThrowsForDlt() {
        NotificationDeliveryDecision decision = sendDecision(4);
        when(deliveries.prepare(event(), AT)).thenReturn(decision);
        org.mockito.Mockito.doThrow(new NotificationSendException(
                NotifyFailureReason.SEND_TIMEOUT, new RuntimeException("timeout")))
                .when(sender).send(decision.notification());
        when(deliveries.completeFailure(decision, NotifyFailureReason.SEND_TIMEOUT, AT, AT))
                .thenReturn(FailureOutcome.TERMINAL);

        assertThatThrownBy(() -> consumer.consume(record(), acknowledgment))
                .isInstanceOf(NotificationTerminalFailureException.class);

        verify(meter).failure();
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void duplicateCompletionAcknowledgesWithoutMetering() {
        NotificationDeliveryDecision decision = sendDecision(1);
        when(deliveries.prepare(event(), AT)).thenReturn(decision);
        org.mockito.Mockito.doThrow(new NotificationSendException(
                NotifyFailureReason.SEND_TIMEOUT, new RuntimeException("timeout")))
                .when(sender).send(decision.notification());
        when(deliveries.completeFailure(decision, NotifyFailureReason.SEND_TIMEOUT, AT, AT))
                .thenReturn(FailureOutcome.DUPLICATE);

        consumer.consume(record(), acknowledgment);

        verify(meter, never()).failure();
        verify(acknowledgment).acknowledge();
    }

    private static NotificationDeliveryDecision sendDecision(int attemptSeq) {
        Notification sending = new Notification(41L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.SENDING, attemptSeq, 0, null,
                "member:20", "coupon-issued:100", AT, AT, null, null);
        return NotificationDeliveryDecision.send(sending, 1, attemptSeq,
                attemptSeq == 1 ? AttemptTrigger.INITIAL : AttemptTrigger.AUTO);
    }

    private static NotificationRequestedEvent event() {
        return new NotificationRequestedEvent(41L, 20L, 10L, 1, AttemptTrigger.INITIAL, AT);
    }

    private static ConsumerRecord<String, NotificationRequestedEvent> record() {
        return new ConsumerRecord<>(KafkaTopicConfig.NOTIFY, 0, 0L, "20", event());
    }
}

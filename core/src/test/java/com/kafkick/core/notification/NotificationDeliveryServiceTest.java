package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.notification.NotificationDeliveryDecision.Action;
import com.kafkick.core.notification.domain.AttemptResult;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationAttempt;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.notification.event.NotificationRequestedEvent;

@ExtendWith(MockitoExtension.class)
class NotificationDeliveryServiceTest {
    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");

    @Mock NotificationRepository notifications;
    @Mock NotificationAttemptRepository attempts;
    private NotificationDeliveryService service;

    @BeforeEach
    void setUp() {
        service = new NotificationDeliveryService(notifications, attempts);
    }

    @Test
    void preparesInitialPendingAttemptWithCas() {
        Notification pending = pending();
        when(notifications.findById(41L)).thenReturn(Optional.of(pending));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of());
        when(notifications.saveIfStatus(any(), any(), any(Integer.class))).thenReturn(true);

        NotificationDeliveryDecision decision = service.prepare(event(1, AttemptTrigger.INITIAL), AT);

        assertThat(decision.action()).isEqualTo(Action.SEND);
        assertThat(decision.attemptSeq()).isEqualTo(1);
        assertThat(decision.trigger()).isEqualTo(AttemptTrigger.INITIAL);
        assertThat(decision.notification().status()).isEqualTo(NotificationStatus.SENDING);
    }

    @Test
    void continuesRetryableFailureAsAutoAttempt() {
        Notification failed = pending().startSending(AttemptTrigger.INITIAL, AT)
                .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(1));
        NotificationAttempt first = failedAttempt(1, AttemptTrigger.INITIAL);
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(first));
        when(notifications.saveIfStatus(any(), any(), any(Integer.class))).thenReturn(true);

        NotificationDeliveryDecision decision = service.prepare(event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.SEND);
        assertThat(decision.attemptSeq()).isEqualTo(2);
        assertThat(decision.trigger()).isEqualTo(AttemptTrigger.AUTO);
    }

    @Test
    void rejectsAnOlderCommandAfterManualLineageStarted() {
        Notification failed = new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.FAILED, 2, 1, NotifyFailureReason.SEND_TIMEOUT,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(
                failedAttempt(1, AttemptTrigger.INITIAL), failedAttempt(2, AttemptTrigger.MANUAL)));

        NotificationDeliveryDecision decision = service.prepare(event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.ACKNOWLEDGE);
    }

    @Test
    void completedCurrentAttemptIsAcknowledgedWithoutAnotherSenderCall() {
        Notification sending = pending().startSending(AttemptTrigger.INITIAL, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(sending));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(
                failedAttempt(1, AttemptTrigger.INITIAL)));

        NotificationDeliveryDecision decision = service.prepare(
                event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.ACKNOWLEDGE);
    }

    @Test
    void completedAttemptInsertWinnerAloneMarksSuccess() {
        Notification sending = pending().startSending(AttemptTrigger.INITIAL, AT);
        NotificationDeliveryDecision decision = NotificationDeliveryDecision.send(sending, 1,
                1, AttemptTrigger.INITIAL);
        when(attempts.saveIfAbsent(any())).thenReturn(true);
        when(notifications.saveIfStatus(any(), any(), any(Integer.class))).thenReturn(true);

        boolean won = service.completeSuccess(decision, AT, AT.plusSeconds(1));

        assertThat(won).isTrue();
        ArgumentCaptor<Notification> next = ArgumentCaptor.forClass(Notification.class);
        verify(notifications).saveIfStatus(next.capture(),
                org.mockito.ArgumentMatchers.eq(NotificationStatus.SENDING),
                org.mockito.ArgumentMatchers.eq(1));
        assertThat(next.getValue().status()).isEqualTo(NotificationStatus.SENT);
    }

    @Test
    void fourthFailureIsTerminalAndEarlierFailureRetries() {
        Notification firstSending = pending().startSending(AttemptTrigger.INITIAL, AT);
        when(attempts.saveIfAbsent(any())).thenReturn(true);
        when(notifications.saveIfStatus(any(), any(), any(Integer.class))).thenReturn(true);

        FailureOutcome first = service.completeFailure(
                NotificationDeliveryDecision.send(firstSending, 1, 1, AttemptTrigger.INITIAL),
                NotifyFailureReason.SEND_TIMEOUT, AT, AT.plusSeconds(1));
        Notification fourthSending = new Notification(41L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.SENDING, 4, 0, null,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        FailureOutcome fourth = service.completeFailure(
                NotificationDeliveryDecision.send(fourthSending, 1, 4, AttemptTrigger.AUTO),
                NotifyFailureReason.SEND_TIMEOUT, AT, AT.plusSeconds(1));

        assertThat(first).isEqualTo(FailureOutcome.RETRY);
        assertThat(fourth).isEqualTo(FailureOutcome.TERMINAL);
    }

    private static Notification pending() {
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 0, 0, null, "member:20", "coupon-issued:100",
                AT, AT, null, null);
    }

    private static NotificationAttempt failedAttempt(int sequence, AttemptTrigger trigger) {
        return new NotificationAttempt((long) sequence, 41L, sequence, trigger,
                AttemptResult.FAILED, NotifyFailureReason.SEND_TIMEOUT,
                AT, AT.plusSeconds(1), AT.plusSeconds(1));
    }

    private static NotificationRequestedEvent event(int sequence, AttemptTrigger trigger) {
        return new NotificationRequestedEvent(41L, 20L, 10L, sequence, trigger, AT);
    }
}

package com.kafkick.core.notification.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import com.kafkick.core.support.exception.BusinessException;

class NotificationTest {
    private static final Instant AT = Instant.parse("2026-08-27T00:00:00Z");

    @Test
    void allowsSevenStateTransitions() {
        Notification pending = pending();
        Notification sending = pending.startSending(AttemptTrigger.INITIAL, AT.plusSeconds(1));
        assertThat(sending.status()).isEqualTo(NotificationStatus.SENDING);
        assertThat(sending.markSent(AT.plusSeconds(2)).status()).isEqualTo(NotificationStatus.SENT);

        Notification failed = sending.markFailed(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(2));
        Notification retrying = failed.startSending(AttemptTrigger.AUTO, AT.plusSeconds(3));
        assertThat(retrying.status()).isEqualTo(NotificationStatus.SENDING);
        assertThat(retrying.markDead(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(4)).status())
                .isEqualTo(NotificationStatus.DEAD);
        assertThat(failed.markDead(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(4)).status())
                .isEqualTo(NotificationStatus.DEAD);
        assertThat(failed.markDead(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(4))
                .startSending(AttemptTrigger.MANUAL, AT.plusSeconds(5)).status())
                .isEqualTo(NotificationStatus.SENDING);
    }

    @Test
    void rejectsLeavingSentAndAutomaticDeadRetry() {
        Notification sent = pending().startSending(AttemptTrigger.INITIAL, AT).markSent(AT);
        assertThatThrownBy(() -> sent.startSending(AttemptTrigger.MANUAL, AT))
                .isInstanceOf(NotificationInvalidTransitionException.class);
        Notification dead = pending().startSending(AttemptTrigger.INITIAL, AT)
                .markDead(NotifyFailureReason.INVALID_RECIPIENT, AT);
        assertThatThrownBy(() -> dead.startSending(AttemptTrigger.AUTO, AT))
                .isInstanceOf(NotificationInvalidTransitionException.class);
        assertThatThrownBy(() -> pending().startSending(AttemptTrigger.MANUAL, AT))
                .isInstanceOf(NotificationInvalidTransitionException.class);
        Notification failed = pending().startSending(AttemptTrigger.INITIAL, AT)
                .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT);
        assertThatThrownBy(() -> failed.startSending(AttemptTrigger.INITIAL, AT))
                .isInstanceOf(NotificationInvalidTransitionException.class);
    }

    @Test
    void resumesOnlyTheSameAttemptWhileSendingWithoutIncrementingCount() {
        Notification sending = pending().startSending(AttemptTrigger.INITIAL, AT);

        Notification resumed = sending.resumeSending(1, AT.plusSeconds(1));

        assertThat(resumed.status()).isEqualTo(NotificationStatus.SENDING);
        assertThat(resumed.attemptCount()).isEqualTo(1);
        assertThatThrownBy(() -> sending.resumeSending(2, AT.plusSeconds(1)))
                .isInstanceOf(NotificationInvalidTransitionException.class);
    }

    @Test
    void successfulManualResendClearsPriorFailureTime() {
        Notification failed = pending().startSending(AttemptTrigger.INITIAL, AT)
                .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(1));

        Notification sent = failed.startSending(AttemptTrigger.MANUAL, AT.plusSeconds(2))
                .markSent(AT.plusSeconds(3));

        assertThat(sent.failedAt()).isNull();
    }

    @Test
    void stringRepresentationExcludesPayload() {
        assertThat(pending().toString()).doesNotContain("recipient", "message");
    }

    @Test
    void retryableClassificationMatchesPolicy() {
        assertThat(NotifyFailureReason.SEND_TIMEOUT.retryable()).isTrue();
        assertThat(NotifyFailureReason.SEND_UNAVAILABLE.retryable()).isTrue();
        assertThat(NotifyFailureReason.CONNECTION_ERROR.retryable()).isTrue();
        assertThat(NotifyFailureReason.INVALID_RECIPIENT.retryable()).isFalse();
        assertThat(NotifyFailureReason.REJECTED_BY_PROVIDER.retryable()).isFalse();
        assertThat(NotifyFailureReason.SERIALIZATION_ERROR.retryable()).isFalse();
        assertThat(NotifyFailureReason.OUTBOX_PUBLISH_FAILED.retryable()).isFalse();
        assertThat(NotifyFailureReason.UNKNOWN.retryable()).isFalse();
    }

    @Test
    void fourthManualResendIsRejected() {
        Notification notification = pending().startSending(AttemptTrigger.INITIAL, AT)
                .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT);
        for (int resend = 0; resend < 3; resend++) {
            notification = notification.startSending(AttemptTrigger.MANUAL, AT)
                    .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT);
        }
        Notification exhausted = notification;
        assertThatThrownBy(() -> exhausted.startSending(AttemptTrigger.MANUAL, AT))
                .isInstanceOf(NotificationResendLimitExceededException.class);
    }

    @Test
    void rejectsPayloadBeyondPlaintextContractBeforeDatabaseWrite() {
        assertThatThrownBy(() -> Notification.pending(
                1L, 2L, 3L, "r".repeat(256), "message", AT))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> Notification.pending(
                1L, 2L, 3L, "recipient", "가".repeat(501), AT))
                .isInstanceOf(BusinessException.class);
    }

    @Test
    void rejectsStorageIncompatibleChannelResendCountAndFailureTime() {
        assertThatThrownBy(() -> new Notification(null, 1L, 2L, 3L, "SMS",
                NotificationStatus.PENDING, 0, 0, null, "recipient", "message",
                AT, AT, null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Notification(null, 1L, 2L, 3L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 0, 4, null, "recipient", "message",
                AT, AT, null, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Notification(null, 1L, 2L, 3L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.FAILED, 1, 0, NotifyFailureReason.SEND_TIMEOUT,
                "recipient", "message", AT, AT, null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsBlankRecipientAndMessage() {
        assertThatThrownBy(() -> Notification.pending(1L, 2L, 3L, " ", "message", AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Notification.pending(1L, 2L, 3L, "recipient", " ", AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Notification pending() {
        return Notification.pending(1L, 2L, 3L, "recipient", "message", AT);
    }
}

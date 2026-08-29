package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.kafkick.core.notification.domain.NotifyFailureReason;

class NotificationSendExceptionTest {
    @Test
    void carriesRetryPolicyReason() {
        NotificationSendException exception = new NotificationSendException(
                NotifyFailureReason.SEND_TIMEOUT, new RuntimeException("timeout"));
        assertThat(exception.reason()).isEqualTo(NotifyFailureReason.SEND_TIMEOUT);
        assertThat(exception.reason().retryable()).isTrue();
    }
}

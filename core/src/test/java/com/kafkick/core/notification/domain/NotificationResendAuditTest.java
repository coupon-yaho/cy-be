package com.kafkick.core.notification.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class NotificationResendAuditTest {
    private static final Instant AT = Instant.parse("2026-08-27T00:00:00Z");

    @Test
    void rejectedRequestHasNoAttemptSequence() {
        NotificationResendAudit audit = new NotificationResendAudit(
                null, 99L, null, 7L, AT, false, "ADMIN-006", AT);
        assertThat(audit.attemptSeq()).isNull();
    }

    @Test
    void acceptedRequestRequiresAttemptSequence() {
        assertThatThrownBy(() -> new NotificationResendAudit(
                null, 99L, null, 7L, AT, true, null, AT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

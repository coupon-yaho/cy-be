package com.kafkick.core.notification.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;

import org.junit.jupiter.api.Test;

class NotificationOutboxTest {
    private static final Instant AT = Instant.parse("2026-08-27T00:00:00Z");

    @Test
    void pendingCommandBecomesPublished() {
        NotificationOutbox pending = NotificationOutbox.pending(
                1L, 2, AttemptTrigger.MANUAL, AT);
        assertThat(pending.status()).isEqualTo(NotificationOutboxStatus.PENDING);
    }

    @Test
    void claimRejectsAutoTrigger() {
        assertThatThrownBy(() -> new NotificationOutboxClaim(1L, 2L, 1,
                AttemptTrigger.AUTO, "claim-token", AT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

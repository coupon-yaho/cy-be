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
                AttemptTrigger.AUTO, "claim-token", AT, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * 스키마의 {@code ck_notification_outbox_failure_count} 와 같은 범위다.
     * 둘이 갈리면 DB 가 받아들인 행을 도메인이 거부해 선점이 통째로 막힌다.
     */
    @Test
    void claimRejectsFailureCountOutsideTheSchemaRange() {
        assertThatThrownBy(() -> new NotificationOutboxClaim(1L, 2L, 1,
                AttemptTrigger.INITIAL, "claim-token", AT, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NotificationOutboxClaim(1L, 2L, 1,
                AttemptTrigger.INITIAL, "claim-token", AT, 11))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

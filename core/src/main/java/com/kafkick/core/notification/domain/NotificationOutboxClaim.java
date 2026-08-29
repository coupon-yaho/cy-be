package com.kafkick.core.notification.domain;

import java.util.Objects;

public record NotificationOutboxClaim(Long outboxId, Long notificationId, int attemptSeq,
        AttemptTrigger trigger, String claimToken) {
    public NotificationOutboxClaim {
        if (outboxId == null || outboxId <= 0 || notificationId == null || notificationId <= 0
                || attemptSeq < 1) {
            throw new IllegalArgumentException("outbox claim 식별자는 양수여야 합니다.");
        }
        Objects.requireNonNull(trigger, "trigger");
        if (trigger == AttemptTrigger.AUTO) {
            throw new IllegalArgumentException("outbox claim trigger는 INITIAL 또는 MANUAL이어야 합니다.");
        }
        if (claimToken == null || claimToken.isBlank()) {
            throw new IllegalArgumentException("claimToken은 비어 있을 수 없습니다.");
        }
    }
}

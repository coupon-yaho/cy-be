package com.kafkick.core.notification.domain;

import java.time.Instant;
import java.util.Objects;

public record NotificationOutbox(Long id, Long notificationId, int attemptSeq,
        AttemptTrigger trigger, NotificationOutboxStatus status,
        Instant createdAt, Instant publishedAt) {
    public NotificationOutbox {
        if (notificationId == null || notificationId <= 0 || attemptSeq < 1) {
            throw new IllegalArgumentException("발행 명령 식별자와 시도 번호는 양수여야 합니다.");
        }
        if (trigger != AttemptTrigger.INITIAL && trigger != AttemptTrigger.MANUAL) {
            throw new IllegalArgumentException("outbox trigger는 INITIAL 또는 MANUAL이어야 합니다.");
        }
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        if ((status == NotificationOutboxStatus.PUBLISHED) != (publishedAt != null)) {
            throw new IllegalArgumentException("발행 상태와 발행 시각은 같이 존재해야 합니다.");
        }
    }

    public static NotificationOutbox pending(Long notificationId, int attemptSeq,
            AttemptTrigger trigger, Instant createdAt) {
        return new NotificationOutbox(null, notificationId, attemptSeq, trigger,
                NotificationOutboxStatus.PENDING, createdAt, null);
    }

}

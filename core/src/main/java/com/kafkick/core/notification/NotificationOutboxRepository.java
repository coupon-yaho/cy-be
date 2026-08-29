package com.kafkick.core.notification;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;

public interface NotificationOutboxRepository {
    NotificationOutbox save(NotificationOutbox outbox);
    Optional<AttemptTrigger> findTriggerByNotificationIdAndAttemptSeq(
            Long notificationId, int attemptSeq);
    Optional<NotificationOutboxClaim> claimNext(Duration lease);
    boolean markPublished(Long outboxId, String claimToken, Instant publishedAt);
    boolean markFailed(Long outboxId, String claimToken, Duration retryDelay);
}

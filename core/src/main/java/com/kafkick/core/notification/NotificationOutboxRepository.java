package com.kafkick.core.notification;

import java.time.Instant;
import java.util.Optional;
import java.time.Duration;

import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;

public interface NotificationOutboxRepository {
    NotificationOutbox save(NotificationOutbox outbox);
    Optional<NotificationOutboxClaim> claimNext(Duration lease);
    boolean markPublished(Long outboxId, String claimToken, Instant publishedAt);
    boolean markFailed(Long outboxId, String claimToken, Duration retryDelay);
}

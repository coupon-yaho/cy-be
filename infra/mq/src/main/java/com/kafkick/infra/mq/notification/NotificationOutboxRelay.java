package com.kafkick.infra.mq.notification;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;

public class NotificationOutboxRelay {

    private static final Duration FAILURE_RETRY_DELAY = Duration.ofSeconds(1);

    private final NotificationOutboxRepository outboxes;
    private final NotificationRepository notifications;
    private final NotificationRequestedEventPublisher publisher;
    private final Duration lease;
    private final Clock clock;

    public NotificationOutboxRelay(NotificationOutboxRepository outboxes,
            NotificationRepository notifications,
            NotificationRequestedEventPublisher publisher,
            Duration lease,
            Clock clock) {
        this.outboxes = Objects.requireNonNull(outboxes, "outboxes");
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public boolean poll() {
        return outboxes.claimNext(lease).map(this::publish).orElse(false);
    }

    private boolean publish(NotificationOutboxClaim claim) {
        Notification notification = notifications.findById(claim.notificationId()).orElse(null);
        if (notification == null) {
            outboxes.markFailed(claim.outboxId(), claim.claimToken(), FAILURE_RETRY_DELAY);
            return false;
        }

        NotificationRequestedEvent event = new NotificationRequestedEvent(
                notification.id(), notification.memberId(), notification.couponId(),
                claim.attemptSeq(), claim.trigger(), claim.requestedAt());
        try {
            publisher.publish(event);
            outboxes.markPublished(claim.outboxId(), claim.claimToken(), clock.instant());
            return true;
        } catch (RuntimeException failure) {
            outboxes.markFailed(claim.outboxId(), claim.claimToken(), FAILURE_RETRY_DELAY);
            return false;
        }
    }
}

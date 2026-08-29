package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;

@ExtendWith(MockitoExtension.class)
class NotificationOutboxRelayTest {
    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");
    private static final Duration LEASE = Duration.ofSeconds(30);

    @Mock NotificationOutboxRepository outboxes;
    @Mock NotificationRepository notifications;
    @Mock NotificationRequestedEventPublisher publisher;
    private NotificationOutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = new NotificationOutboxRelay(outboxes, notifications, publisher, LEASE,
                Clock.fixed(AT, ZoneOffset.UTC));
    }

    @Test
    void emptyPollDoesNothing() {
        when(outboxes.claimNext(LEASE)).thenReturn(Optional.empty());

        assertThat(relay.poll()).isFalse();

        verify(publisher, never()).publish(any());
    }

    @Test
    void publishesClaimAndMarksItWithFencingToken() {
        NotificationOutboxClaim claim = claim();
        when(outboxes.claimNext(LEASE)).thenReturn(Optional.of(claim));
        when(notifications.findById(41L)).thenReturn(Optional.of(notification()));

        assertThat(relay.poll()).isTrue();

        ArgumentCaptor<NotificationRequestedEvent> event =
                ArgumentCaptor.forClass(NotificationRequestedEvent.class);
        verify(publisher).publish(event.capture());
        assertThat(event.getValue().notificationId()).isEqualTo(41L);
        assertThat(event.getValue().requestedAt()).isEqualTo(AT);
        verify(outboxes).markPublished(7L, "token", AT);
    }

    @Test
    void publishFailureReturnsClaimToPendingAfterOneSecond() {
        when(outboxes.claimNext(LEASE)).thenReturn(Optional.of(claim()));
        when(notifications.findById(41L)).thenReturn(Optional.of(notification()));
        org.mockito.Mockito.doThrow(new IllegalStateException("broker unavailable"))
                .when(publisher).publish(any());

        assertThat(relay.poll()).isFalse();

        verify(outboxes).markFailed(7L, "token", Duration.ofSeconds(1));
    }

    private static NotificationOutboxClaim claim() {
        return new NotificationOutboxClaim(7L, 41L, 1,
                AttemptTrigger.INITIAL, "token", AT);
    }

    private static Notification notification() {
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 0, 0, null, "member:20", "coupon-issued:100",
                AT, AT, null, null);
    }
}

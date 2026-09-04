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
import java.util.List;
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
    private static final Duration BASE = Duration.ofMillis(200);
    private static final Duration CAP = Duration.ofSeconds(20);
    private static final int BATCH = 64;

    @Mock NotificationOutboxRepository outboxes;
    @Mock NotificationRepository notifications;
    @Mock NotificationRequestedEventPublisher publisher;
    private NotificationOutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = new NotificationOutboxRelay(outboxes, notifications, publisher, LEASE,
                BATCH, new FullJitterBackOff(BASE, CAP), Clock.fixed(AT, ZoneOffset.UTC));
    }

    @Test
    void emptyPollDoesNothing() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of());

        assertThat(relay.poll()).isZero();

        verify(publisher, never()).publish(any());
    }

    @Test
    void publishesClaimAndMarksItWithFencingToken() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim()));
        when(notifications.findById(41L)).thenReturn(Optional.of(notification()));

        assertThat(relay.poll()).isEqualTo(1);

        ArgumentCaptor<NotificationRequestedEvent> event =
                ArgumentCaptor.forClass(NotificationRequestedEvent.class);
        verify(publisher).publish(event.capture());
        assertThat(event.getValue().notificationId()).isEqualTo(41L);
        assertThat(event.getValue().requestedAt()).isEqualTo(AT);
        verify(outboxes).markPublished(7L, "token", AT);
    }

    /**
     * 예전에는 여기가 {@code Duration.ofSeconds(1)} 상수 하나였다. 그래서 같이 실패한 것들이
     * <b>같은 시각으로</b> 예약됐다. 이제는 값이 아니라 <b>범위</b>를 단언한다 —
     * 상한은 {@code base × 2^(failureCount+1)} 이다.
     */
    @Test
    void publishFailureReturnsClaimToPendingAfterAJitteredDelay() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findById(41L)).thenReturn(Optional.of(notification()));
        org.mockito.Mockito.doThrow(new IllegalStateException("broker unavailable"))
                .when(publisher).publish(any());

        assertThat(relay.poll()).isZero();

        assertThat(capturedRetryDelay())
                .isBetween(Duration.ZERO, BASE.multipliedBy(2));
    }

    /** 발행 대상이 사라진 경로도 같은 지연을 쓴다. 한쪽만 지터를 주면 나머지가 다시 뭉친다. */
    @Test
    void missingNotificationAlsoUsesTheJitteredDelay() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findById(41L)).thenReturn(Optional.empty());

        assertThat(relay.poll()).isZero();

        assertThat(capturedRetryDelay())
                .isBetween(Duration.ZERO, BASE.multipliedBy(2));
    }

    /**
     * <b>{@code failureCount} 는 이번 실패를 세기 전 값이다.</b> 그대로 쓰면 상한이 한 칸
     * 작아진다 — 세 번 실패한 건의 상한은 {@code base × 2^3} 이지 {@code 2^2} 가 아니다.
     */
    @Test
    void ceilingGrowsWithTheClaimsFailureCount() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(3)));
        when(notifications.findById(41L)).thenReturn(Optional.empty());

        assertThat(relay.poll()).isZero();

        assertThat(capturedRetryDelay())
                .isBetween(Duration.ZERO, BASE.multipliedBy(1L << 4));
    }

    private Duration capturedRetryDelay() {
        ArgumentCaptor<Duration> delay = ArgumentCaptor.forClass(Duration.class);
        verify(outboxes).markFailed(org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq("token"), delay.capture());
        return delay.getValue();
    }

    private static NotificationOutboxClaim claim() {
        return claim(0);
    }

    private static NotificationOutboxClaim claim(int failureCount) {
        return new NotificationOutboxClaim(7L, 41L, 1,
                AttemptTrigger.INITIAL, "token", AT, failureCount);
    }

    private static Notification notification() {
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 0, 0, null, "member:20", "coupon-issued:100",
                AT, AT, null, null);
    }
}

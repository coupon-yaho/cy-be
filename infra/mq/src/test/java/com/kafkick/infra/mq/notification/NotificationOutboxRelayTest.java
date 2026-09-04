package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    /**
     * <b>배치 전체가 한 lease 를 공유한다.</b> 집는 순간 전부 {@code claimed_at} 이 찍히는데
     * 발행은 차례로 도므로, 뒤쪽 행은 앞쪽을 기다리며 lease 를 태운다. 그 합이 lease 를
     * 넘으면 <b>아직 처리도 안 한 행이 회수되어 남이 같은 이벤트를 다시 발행한다.</b>
     *
     * <p>Qodo 리뷰가 잡았다 — 배치 크기와 lease 를 따로 정할 수 있게 두면 그 조합이
     * 조용히 중복 발행을 만든다.
     */
    @Test
    void rejectsBatchSizeThatWouldOutlastTheLease() {
        // 64 × 100ms = 6.4s 는 30s lease 안쪽이다. 400 × 100ms = 40s 는 넘는다.
        assertThatThrownBy(() -> new NotificationOutboxRelay(outboxes, notifications, publisher,
                LEASE, 400, new FullJitterBackOff(BASE, CAP), Clock.fixed(AT, ZoneOffset.UTC)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lease");
    }

    @Test
    void rejectsNonPositiveBatchSize() {
        assertThatThrownBy(() -> new NotificationOutboxRelay(outboxes, notifications, publisher,
                LEASE, 0, new FullJitterBackOff(BASE, CAP), Clock.fixed(AT, ZoneOffset.UTC)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * <b>어차피 기동이 안 될 것이라면 원인을 말하고 죽는다.</b>
     *
     * <p>상한이 없으면 큰 lease 에서 {@code Duration.toMillis()} 가
     * {@code ArithmeticException} 으로 죽는데, 그 예외는 <b>무엇이 틀렸는지 말하지 않는다.</b>
     * 이 검사는 기동을 막는 것을 피하지 않는다 — 같은 기동 실패를 <b>읽을 수 있게</b> 만든다.
     *
     * <p>상한은 저장소 어댑터가 받는 범위(365일)와 같다. 더 좁히면 저장소가 받는 설정을
     * 릴레이가 거부하게 된다.
     */
    @Test
    void rejectsLeaseTooLargeToConvertSafely() {
        assertThatThrownBy(() -> new NotificationOutboxRelay(outboxes, notifications, publisher,
                Duration.ofSeconds(Long.MAX_VALUE / 1000), BATCH,
                new FullJitterBackOff(BASE, CAP), Clock.fixed(AT, ZoneOffset.UTC)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lease");
    }

    /** 저장소가 받는 범위(365일)는 릴레이도 받아야 한다. 여기서 더 좁히면 설정이 갈린다. */
    @Test
    void acceptsTheSameLeaseRangeTheAdapterSupports() {
        assertThatCode(() -> new NotificationOutboxRelay(outboxes, notifications, publisher,
                Duration.ofDays(365), BATCH, new FullJitterBackOff(BASE, CAP),
                Clock.fixed(AT, ZoneOffset.UTC)))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsNonPositiveLease() {
        assertThatThrownBy(() -> new NotificationOutboxRelay(outboxes, notifications, publisher,
                Duration.ZERO, BATCH, new FullJitterBackOff(BASE, CAP),
                Clock.fixed(AT, ZoneOffset.UTC)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

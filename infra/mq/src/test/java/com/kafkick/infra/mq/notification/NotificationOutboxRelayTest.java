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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

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
    private static final int MAX_IN_FLIGHT = 64;
    private static final int WORKERS = 8;

    /**
     * <b>부른 스레드에서 그대로 돈다.</b> 여기서 재려는 것은 백프레셔와 클레임 처리이지
     * 스레드 풀이 아니다 — 진짜 풀을 쓰면 같은 것을 재면서 타이밍만 흔들린다.
     * 동시성 자체를 재는 것은 아래 {@code drains…}·{@code backpressure…} 쪽이다.
     */
    private static final Executor DIRECT = Runnable::run;

    @Mock NotificationOutboxRepository outboxes;
    @Mock NotificationRepository notifications;
    @Mock NotificationRequestedEventPublisher publisher;
    private NotificationOutboxRelay relay;

    @BeforeEach
    void setUp() {
        relay = relayWith(LEASE, BATCH, DIRECT, MAX_IN_FLIGHT, WORKERS);
    }

    private NotificationOutboxRelay relayWith(Duration lease, int batch, Executor workers,
            int maxInFlight, int workerCount) {
        return new NotificationOutboxRelay(outboxes, notifications, publisher, lease, batch,
                workers, maxInFlight, workerCount, new FullJitterBackOff(BASE, CAP),
                Clock.fixed(AT, ZoneOffset.UTC));
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

        // 반환값은 **넘긴 수**다 — 발행 성공 여부는 아래 markFailed 로 본다.
        assertThat(relay.poll()).isEqualTo(1);

        assertThat(capturedRetryDelay())
                .isBetween(Duration.ZERO, BASE.multipliedBy(2));
    }

    /** 발행 대상이 사라진 경로도 같은 지연을 쓴다. 한쪽만 지터를 주면 나머지가 다시 뭉친다. */
    @Test
    void missingNotificationAlsoUsesTheJitteredDelay() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findById(41L)).thenReturn(Optional.empty());

        // 반환값은 **넘긴 수**다 — 발행 성공 여부는 아래 markFailed 로 본다.
        assertThat(relay.poll()).isEqualTo(1);

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

        // 반환값은 **넘긴 수**다 — 발행 성공 여부는 아래 markFailed 로 본다.
        assertThat(relay.poll()).isEqualTo(1);

        assertThat(capturedRetryDelay())
                .isBetween(Duration.ZERO, BASE.multipliedBy(1L << 4));
    }

    /**
     * <b>백프레셔 — 인플라이트가 상한이면 아무것도 집지 않는다.</b>
     *
     * <p>상한 없이 뿌리면 클레임이 처리 속도를 앞질러 {@code IN_PROGRESS} 가 쌓이고,
     * lease 가 만료되어 <b>회수 → 재클레임 → 다시 만료</b>가 돈다. 일은 안 늘고 DB 쓰기만
     * 는다. 그래서 <b>집기 전에</b> 막는다 — 집고 나서 버리면 이미 lease 를 태운 뒤다.
     *
     * <p>워커를 붙잡아 인플라이트를 상한(2)까지 채운 뒤 한 회차를 더 돌린다. 그 회차는
     * {@code claimBatch} 를 <b>부르지도 않아야</b> 한다.
     */
    @Test
    void backpressureSkipsTheRoundWhenInFlightIsAtTheBound() throws Exception {
        CountDownLatch hold = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Executor blocking = task -> pool.execute(() -> {
                started.countDown();
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                task.run();
            });
            NotificationOutboxRelay bounded = relayWith(LEASE, BATCH, blocking, 2, 2);
            when(outboxes.claimBatch(LEASE, 2)).thenReturn(List.of(claim(), claim()));
            when(notifications.findById(41L)).thenReturn(Optional.of(notification()));

            assertThat(bounded.poll()).isEqualTo(2);
            assertThat(started.await(5, TimeUnit.SECONDS))
                    .as("워커가 두 개 다 붙잡혀야 인플라이트가 상한입니다").isTrue();
            assertThat(bounded.inFlight()).isEqualTo(2);

            // 첫 회차의 호출은 지운다 — 여기서 보려는 것은 **그 다음 회차**다.
            org.mockito.Mockito.clearInvocations(outboxes);

            assertThat(bounded.poll()).as("상한에서는 이번 회차를 건너뜁니다").isZero();
            // 상한이 2 이므로 여유는 0 이다. 어떤 크기로도 집으면 안 된다.
            verify(outboxes, never()).claimBatch(any(), org.mockito.ArgumentMatchers.anyInt());
        } finally {
            hold.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    /**
     * <b>여유보다 큰 배치는 여유만큼으로 잘린다.</b> 자르지 않으면 한 회차가 상한을 넘겨
     * 집고, 넘긴 만큼이 큐에서 lease 를 태운다 — 생성자의 파도 계산이 가정한 폭이 깨진다.
     */
    @Test
    void theClaimIsCappedByTheRemainingInFlightHeadroom() {
        NotificationOutboxRelay bounded = relayWith(LEASE, BATCH, DIRECT, 5, 5);
        when(outboxes.claimBatch(LEASE, 5)).thenReturn(List.of());

        assertThat(bounded.poll()).isZero();

        // 배치는 64 지만 상한이 5 이므로 5 로 잘려 들어가야 한다.
        verify(outboxes).claimBatch(LEASE, 5);
    }

    /**
     * <b>종료 시 인플라이트를 기다린다.</b> 안 기다리면 그 행은 {@code IN_PROGRESS} 로 남아
     * <b>lease 가 만료될 때까지</b> 아무도 못 집는다 — 재기동 직후 그만큼이 지연된다.
     *
     * <p>배수 중에는 새로 집지 않는 것도 함께 본다. 계속 집으면 배수가 영영 안 끝난다.
     */
    @Test
    void awaitDrainWaitsForInFlightAndStopsClaiming() throws Exception {
        CountDownLatch hold = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Executor blocking = task -> pool.execute(() -> {
                started.countDown();
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                task.run();
            });
            NotificationOutboxRelay draining = relayWith(LEASE, BATCH, blocking, 1, 1);
            when(outboxes.claimBatch(LEASE, 1)).thenReturn(List.of(claim()));
            when(notifications.findById(41L)).thenReturn(Optional.of(notification()));

            assertThat(draining.poll()).isEqualTo(1);
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(draining.awaitDrain(Duration.ofMillis(200)))
                    .as("아직 워커가 붙잡혀 있으므로 시간 안에 못 빠집니다").isFalse();
            assertThat(draining.poll()).as("배수 중에는 새로 집지 않습니다").isZero();

            hold.countDown();
            assertThat(draining.awaitDrain(Duration.ofSeconds(5)))
                    .as("워커가 끝났으면 빠져야 합니다").isTrue();
            assertThat(draining.inFlight()).isZero();
            verify(outboxes).markPublished(org.mockito.ArgumentMatchers.eq(7L),
                    org.mockito.ArgumentMatchers.eq("token"), any());
        } finally {
            hold.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    /**
     * <b>풀이 거부하면 인플라이트를 되돌린다.</b> 안 되돌리면 그 자리가 영원히 점유된 것으로
     * 세어져 <b>백프레셔가 점점 좁아지다 릴레이가 아무것도 못 집는 상태로 굳는다</b> —
     * 재기동 전까지 조용히 멈춘다.
     *
     * <p>거부한 건을 {@code markFailed} 로 세지 <b>않는</b> 것도 함께 본다. 거부는 발행
     * 실패가 아니라 <b>보내 보지도 못한 것</b>이라, 세면 {@code failure_count} 가 실제 발행
     * 실패 없이 올라 {@code DEAD} 로 가는 시간을 앞당긴다. 그 행은 lease 만료로 회수된다.
     */
    @Test
    void aRejectedDispatchReleasesItsInFlightSlot() {
        Executor rejecting = task -> {
            throw new RejectedExecutionException("pool full");
        };
        NotificationOutboxRelay relay = relayWith(LEASE, BATCH, rejecting, 4, 4);
        when(outboxes.claimBatch(LEASE, 4)).thenReturn(List.of(claim()));

        assertThatThrownBy(relay::poll).isInstanceOf(RejectedExecutionException.class);

        assertThat(relay.inFlight()).isZero();
        verify(outboxes, never()).markFailed(org.mockito.ArgumentMatchers.anyLong(),
                any(), any());
    }

    /**
     * <b>늦은 워커의 결과는 버려진다.</b> lease 가 만료되어 남이 다시 집은 뒤에 끝난 워커는
     * {@code claim_token} 이 안 맞아 <b>0행</b>을 고친다. 저장소가 그것을 {@code false} 로
     * 돌려주는 것은 이미 재고 있고({@code NotificationOutboxRepositoryTest}), 여기서 보는
     * 것은 <b>릴레이가 그 false 를 실패로 되받지 않는다</b>는 쪽이다.
     *
     * <p>되받으면 {@code markFailed} 가 불려 <b>남의 클레임에 실패를 적으려 든다</b> —
     * 그것도 토큰이 안 맞아 0행이므로 조용히 아무 일도 안 일어나지만, {@code failure_count}
     * 를 올리려는 시도 자체가 잘못된 모델이다. 이 건은 <b>이미 남의 것</b>이다.
     *
     * <p>워커 풀이 생기기 전에는 이 경합이 거의 안 났다 — 한 번에 한 건이었고 lease 는
     * 30초였다. 동시에 여러 건을 붙잡게 되면서 실제 경로가 됐다.
     */
    @Test
    void aStaleWorkerResultIsDiscardedRatherThanRetried() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim()));
        when(notifications.findById(41L)).thenReturn(Optional.of(notification()));
        // 그 사이 lease 가 만료되어 남이 다시 집었다 — 토큰이 안 맞아 0행이다.
        when(outboxes.markPublished(org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq("token"), any())).thenReturn(false);

        assertThat(relay.poll()).isEqualTo(1);

        verify(outboxes, never()).markFailed(org.mockito.ArgumentMatchers.anyLong(),
                any(), any());
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
     * <b>한 회차에 집은 것이 한 lease 를 공유한다.</b> 집는 순간 전부 {@code claimed_at} 이
     * 찍히는데 워커는 유한하므로, 워커 수를 넘는 만큼은 큐에서 기다리며 lease 를 태운다.
     * 마지막 파도가 lease 를 넘으면 <b>아직 처리도 안 한 행이 회수되어 남이 같은 이벤트를
     * 다시 발행한다.</b>
     *
     * <p>Qodo 리뷰가 잡았다 — 크기와 lease 를 따로 정할 수 있게 두면 그 조합이 조용히
     * 중복 발행을 만든다.
     */
    @Test
    void rejectsAnInFlightBoundThatWouldOutlastTheLease() {
        // 워커 1: 400 파도 × 100ms = 40s 로 30s lease 를 넘는다.
        assertThatThrownBy(() -> relayWith(LEASE, BATCH, DIRECT, 400, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lease");
    }

    /**
     * <b>동시성을 넣었으면 검사도 같이 넓어져야 한다.</b> 워커가 여덟이면 같은 400건이
     * 50 파도(5초)라 30초 lease 안쪽이다 — 여기서 옛 식(집는 수 × 예산)을 그대로 두면
     * <b>풀을 붙이고도 예전만큼만 허용한다.</b>
     *
     * <p>위 테스트와 인플라이트 상한이 같고 워커 수만 다르다. 그래야 이 둘이 <b>워커 수가
     * 판정을 바꾼다</b>는 사실 하나만 가리킨다.
     */
    @Test
    void moreWorkersMakeTheSameInFlightBoundAcceptable() {
        assertThatCode(() -> relayWith(LEASE, BATCH, DIRECT, 400, 8))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsNonPositiveBatchSize() {
        assertThatThrownBy(() -> relayWith(LEASE, 0, DIRECT, MAX_IN_FLIGHT, WORKERS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 0 이면 백프레셔가 항상 걸려 <b>아무것도 집지 않고 조용히 정상으로 보인다.</b> */
    @Test
    void rejectsNonPositiveInFlightBound() {
        assertThatThrownBy(() -> relayWith(LEASE, BATCH, DIRECT, 0, WORKERS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 0 이면 풀이 아무것도 실행하지 못해 인플라이트가 상한에 붙은 채 굳는다. */
    @Test
    void rejectsNonPositiveWorkerCount() {
        assertThatThrownBy(() -> relayWith(LEASE, BATCH, DIRECT, MAX_IN_FLIGHT, 0))
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
        assertThatThrownBy(() -> relayWith(Duration.ofSeconds(Long.MAX_VALUE / 1000), BATCH,
                DIRECT, MAX_IN_FLIGHT, WORKERS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lease");
    }

    /** 저장소가 받는 범위(365일)는 릴레이도 받아야 한다. 여기서 더 좁히면 설정이 갈린다. */
    @Test
    void acceptsTheSameLeaseRangeTheAdapterSupports() {
        assertThatCode(() -> relayWith(Duration.ofDays(365), BATCH, DIRECT, MAX_IN_FLIGHT,
                WORKERS))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsNonPositiveLease() {
        assertThatThrownBy(() -> relayWith(Duration.ZERO, BATCH, DIRECT, MAX_IN_FLIGHT, WORKERS))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
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
import com.kafkick.core.notification.OutboxRetryReason;
import com.kafkick.core.notification.retry.FullJitterBackOff;
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
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));

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
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));
        org.mockito.Mockito.doThrow(new IllegalStateException("broker unavailable"))
                .when(publisher).publish(any());

        // 반환값은 **넘긴 수**다 — 발행 성공 여부는 아래 markFailed 로 본다.
        assertThat(relay.poll()).isEqualTo(1);

        assertThat(capturedRetryDelay())
                .isBetween(Duration.ZERO, BASE.multipliedBy(2));
    }

    /**
     * <b>발행은 성공했는데 기록이 실패한 것은 발행 실패가 아니다.</b>
     *
     * <p>한때 {@code try} 하나가 발행과 기록을 함께 감쌌다. 그래서 이 경우에도
     * {@code PUBLISH_FAILED} 가 붙었고, 지표는 <b>성공한 발행</b>을 발행 실패로 셌다 —
     * 운영자가 그 이름을 보고 카프카를 뒤지는데 문제는 DB 다. 더 나쁜 것은
     * {@code failure_count} 다: 열 번이면 그 명령이 <i>"사람 손이 필요한 건수"</i> 에
     * 오르는데 <b>그 알림은 나갔다</b>(열 번이 다 이 사유라는 뜻은 아니다 —
     * {@code failure_count} 는 사유와 무관하게 누적된다).
     */
    @Test
    void aFailedRecordIsNotAFailedPublish() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));
        org.mockito.Mockito.doThrow(new IllegalStateException("db down"))
                .when(outboxes).markPublished(anyLong(), anyString(), any());

        relay.poll();

        org.mockito.Mockito.verify(publisher).publish(any());
        assertThat(capturedReason())
                .as("발행은 나갔다. 그것을 publish_failed 로 세면 원인을 엉뚱한 데서 찾는다")
                .isEqualTo(OutboxRetryReason.RECORD_FAILED);
    }

    /**
     * <b>펜싱을 지면 되돌리지 않고 <i>센다</i>.</b>
     *
     * <p>{@code markPublished} 가 {@code false} 를 주는 것은 {@code claim_token} 이
     * 안 맞는다는 뜻 — lease 가 만료돼 남이 가져갔다. 그 행은 이미 우리 것이 아니라
     * {@code markFailed} 를 불러 봐야 <b>같은 토큰 검사에서 0행</b>이라 아무 일도
     * 안 일어난다. 그래서 되돌리지 않는다.
     *
     * <p><b>세는 것은 어댑터가 한다.</b> 갱신이 먹었는지는 그쪽만 알기 때문이다 —
     * 그 축은 {@code NotificationOutboxFenceLostTest} 가 진짜 DB 로 잰다. 여기서는
     * 릴레이의 몫, 즉 <b>되돌리지 않는다</b> 만 본다.
     */
    @Test
    void aLostFenceIsNotReturnedToPending() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));
        when(outboxes.markPublished(anyLong(), anyString(), any())).thenReturn(false);

        relay.poll();

        org.mockito.Mockito.verify(publisher).publish(any());
        verify(outboxes, never()).markFailed(anyLong(), anyString(), any(), any());
    }

    /** 발행 자체가 던진 경우는 그대로 {@code PUBLISH_FAILED} 다 — 갈랐다고 뭉개지 않는다. */
    @Test
    void aThrowingPublishIsStillAPublishFailure() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));
        org.mockito.Mockito.doThrow(new IllegalStateException("broker unavailable"))
                .when(publisher).publish(any());

        relay.poll();

        verify(outboxes, never()).markPublished(anyLong(), anyString(), any());
        assertThat(capturedReason()).isEqualTo(OutboxRetryReason.PUBLISH_FAILED);
    }

    /** 정상 경로에서는 아무것도 안 되돌린다. */
    @Test
    void aWonFenceReturnsNothingToPending() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));
        when(outboxes.markPublished(anyLong(), anyString(), any())).thenReturn(true);

        relay.poll();

        verify(outboxes, never()).markFailed(anyLong(), anyString(), any(), any());
    }

    private OutboxRetryReason capturedReason() {
        org.mockito.ArgumentCaptor<OutboxRetryReason> reason =
                org.mockito.ArgumentCaptor.forClass(OutboxRetryReason.class);
        verify(outboxes).markFailed(anyLong(), anyString(), any(), reason.capture());
        return reason.getValue();
    }

    /** 발행 대상이 사라진 경로도 같은 지연을 쓴다. 한쪽만 지터를 주면 나머지가 다시 뭉친다. */
    @Test
    void missingNotificationAlsoUsesTheJitteredDelay() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of());

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
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of());

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
            when(notifications.findAllByIdIn(org.mockito.ArgumentMatchers.anyList())).thenReturn(List.of(notification()));

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
            when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));

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
                any(), any(), any());
    }

    /**
     * <b>배치 중간에 거부되면 아직 안 넘긴 나머지까지 되돌린다.</b>
     *
     * <p>{@code claimBatch} 는 <b>전부 먼저 선점한다.</b> 그래서 제출 루프가 중간에 멈추면
     * 남은 행은 넘겨지지도, 실패로 적히지도 않은 채 {@code IN_PROGRESS} 로 남는다 —
     * <b>lease 가 만료될 때까지</b>(기본 30초) 아무도 못 집는다. 리뷰가 짚었고, 그때의
     * 거부 테스트는 클레임이 하나뿐이라 이 부분 배치 지연을 못 봤다.
     *
     * <p>되돌리는 것은 {@code markFailed} 가 아니라 {@code releaseClaim} 이다 — 거부는
     * 발행 실패가 아니라 <b>시작도 못 한 것</b>이라, 실패로 세면 거부가 잦은 순간에
     * <b>한 번도 안 보낸 알림이 {@code DEAD} 로 간다.</b>
     */
    @Test
    void aRejectionMidBatchReleasesTheStrandedRemainder() {
        java.util.concurrent.atomic.AtomicInteger accepted =
                new java.util.concurrent.atomic.AtomicInteger();
        Executor rejectsAfterFirst = task -> {
            if (accepted.getAndIncrement() >= 1) {
                throw new RejectedExecutionException("pool full");
            }
            task.run();
        };
        NotificationOutboxRelay relay = relayWith(LEASE, BATCH, rejectsAfterFirst, 4, 4);
        when(outboxes.claimBatch(LEASE, 4))
                .thenReturn(List.of(claim(1L), claim(2L), claim(3L)));
        when(notifications.findAllByIdIn(org.mockito.ArgumentMatchers.anyList())).thenReturn(List.of(notification()));

        assertThatThrownBy(relay::poll).isInstanceOf(RejectedExecutionException.class);

        // 첫 건은 실제로 돌았고, 거부된 2번과 아직 못 넘긴 3번이 되돌려져야 한다.
        verify(outboxes).releaseClaim(2L, "token-2");
        verify(outboxes).releaseClaim(3L, "token-3");
        verify(outboxes, never()).releaseClaim(1L, "token-1");
        verify(outboxes, never()).markFailed(org.mockito.ArgumentMatchers.anyLong(),
                any(), any(), any());
        assertThat(relay.inFlight()).isZero();
    }

    /**
     * <b>되돌리는 문장이 던져도 나머지는 계속 되돌린다.</b>
     *
     * <p>{@code releaseClaim} 자체가 실패할 수 있다(락 경합·연결 끊김). 거기서 루프가
     * 멈추면 그 뒤의 클레임이 또 lease 만료까지 {@code IN_PROGRESS} 로 남는다 —
     * <b>되돌리기를 넣은 이유가 바로 그것인데 같은 구멍을 한 겹 안쪽에 다시 파는 셈이다.</b>
     * 리뷰가 짚었다.
     *
     * <p>실패는 원래 거부에 {@code suppressed} 로 매달린다. 삼키면 되돌리기가 실패한
     * 사실이 안 남고, 대신 던지면 진짜 원인인 거부가 가려진다.
     */
    @Test
    void aFailingReleaseDoesNotStrandTheRestOfTheBatch() {
        Executor rejecting = task -> {
            throw new RejectedExecutionException("pool full");
        };
        NotificationOutboxRelay relay = relayWith(LEASE, BATCH, rejecting, 4, 4);
        when(outboxes.claimBatch(LEASE, 4))
                .thenReturn(List.of(claim(1L), claim(2L), claim(3L)));
        when(outboxes.releaseClaim(1L, "token-1"))
                .thenThrow(new IllegalStateException("lock wait timeout"));

        assertThatThrownBy(relay::poll)
                .isInstanceOf(RejectedExecutionException.class)
                .satisfies(thrown -> assertThat(thrown.getSuppressed())
                        .as("되돌리기가 실패한 사실이 아무 데도 안 남으면 안 됩니다")
                        .hasSize(1));

        verify(outboxes).releaseClaim(2L, "token-2");
        verify(outboxes).releaseClaim(3L, "token-3");
    }

    /**
     * <b>배수 대기 상한을 넘기면 깃발을 세우기 전에 거절한다.</b>
     *
     * <p>{@link Duration#toNanos()} 가 큰 값에서 던지는데, 그 예외가
     * {@code stopping} 을 세운 <b>뒤에</b> 터지면 <b>배수도 못 끝낸 릴레이가 영영 새로 안
     * 집는 상태</b>로 남는다. 그래서 검사가 대입보다 앞이어야 한다 — 이 테스트는
     * 예외가 나는 것뿐 아니라 <b>그 뒤에도 릴레이가 계속 돈다</b>는 것까지 본다.
     */
    @Test
    void anOutOfRangeDrainTimeoutIsRejectedBeforeTheStopFlagIsSet() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of());

        assertThatThrownBy(() -> relay.awaitDrain(Duration.ofDays(400)))
                .isInstanceOf(IllegalArgumentException.class);

        assertThat(relay.isRunning())
                .as("거절이 릴레이를 멈춘 상태로 남기면 그 뒤로 영영 새로 안 집습니다").isTrue();
        assertThat(relay.poll()).isZero();
        verify(outboxes).claimBatch(LEASE, BATCH);
    }

    /**
     * <b>배수가 끝난 뒤에는 이미 진행 중이던 폴링도 새로 집지 못한다.</b>
     *
     * <p>{@code stopping} 검사와 클레임이 원자적으로 묶이지 않으면 이런 순서가 가능하다 —
     * 폴링이 {@code false} 를 읽고, 종료 스레드가 깃발을 세우고 인플라이트 0 을 보고
     * <b>배수 완료로 판정</b>하고, 그 뒤 폴링이 새로 집어 제출한다. {@code fixedDelay} 는
     * 폴링끼리만 직렬화하지 종료 스레드와는 아무 관계가 없다. 리뷰가 짚었다.
     *
     * <p>여기서는 <b>클레임 도중에</b> 다른 스레드가 배수를 끝내게 만든다. 게이트가
     * 없으면 그 폴링이 집은 것을 그대로 제출해 <b>배수 뒤 인플라이트</b>가 생긴다.
     */
    @Test
    void aPollAlreadyInProgressCannotClaimPastACompletedDrain() throws Exception {
        CountDownLatch claiming = new CountDownLatch(1);
        CountDownLatch drained = new CountDownLatch(1);
        when(outboxes.claimBatch(LEASE, BATCH)).thenAnswer(invocation -> {
            claiming.countDown();
            // 게이트가 없다면 이 사이에 배수가 끝난다.
            assertThat(drained.await(2, TimeUnit.SECONDS)).isFalse();
            return List.of(claim());
        });
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));

        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            var polling = caller.submit(relay::poll);
            assertThat(claiming.await(5, TimeUnit.SECONDS)).isTrue();

            // 게이트를 잡을 때까지 여기서 막힌다 — 그것이 이 테스트의 요점이다.
            assertThat(relay.awaitDrain(Duration.ofSeconds(5))).isTrue();
            drained.countDown();

            assertThat(polling.get(5, TimeUnit.SECONDS))
                    .as("게이트가 없으면 배수가 끝난 뒤에 이 폴링이 제출합니다").isEqualTo(1);
            assertThat(relay.inFlight())
                    .as("배수가 끝났는데 인플라이트가 남아 있으면 종료 보장이 깨진 것입니다")
                    .isZero();
            assertThat(relay.poll()).isZero();
        } finally {
            caller.shutdownNow();
        }
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
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));
        // 그 사이 lease 가 만료되어 남이 다시 집었다 — 토큰이 안 맞아 0행이다.
        when(outboxes.markPublished(org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq("token"), any())).thenReturn(false);

        assertThat(relay.poll()).isEqualTo(1);

        verify(outboxes, never()).markFailed(org.mockito.ArgumentMatchers.anyLong(),
                any(), any(), any());
    }

    /**
     * <b>두 실패 경로가 서로 다른 사유로 넘어간다.</b>
     *
     * <p>첫 판은 릴레이가 직접 셌는데 틀렸다 — 릴레이는 그 쓰기가 먹었는지도, 상한을
     * 넘겨 종착했는지도 모른다. 세는 것은 결과를 아는 저장소가 하고, <b>사유만</b>
     * 여기서 넘어간다. 그래서 여기서 잴 것은 "무엇이 넘어갔는가" 하나다.
     */
    @Test
    void theMissingNotificationPathPassesItsOwnReason() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of());

        relay.poll();

        verify(outboxes).markFailed(org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq("token"), any(),
                org.mockito.ArgumentMatchers.eq(OutboxRetryReason.NOTIFICATION_MISSING));
    }

    @Test
    void thePublishFailurePathPassesItsOwnReason() {
        when(outboxes.claimBatch(LEASE, BATCH)).thenReturn(List.of(claim(0)));
        when(notifications.findAllByIdIn(List.of(41L))).thenReturn(List.of(notification()));
        org.mockito.Mockito.doThrow(new IllegalStateException("broker unavailable"))
                .when(publisher).publish(any());

        relay.poll();

        verify(outboxes).markFailed(org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq("token"), any(),
                org.mockito.ArgumentMatchers.eq(OutboxRetryReason.PUBLISH_FAILED));
    }

    private Duration capturedRetryDelay() {
        ArgumentCaptor<Duration> delay = ArgumentCaptor.forClass(Duration.class);
        verify(outboxes).markFailed(org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq("token"), delay.capture(), any());
        return delay.getValue();
    }

    private static NotificationOutboxClaim claim() {
        return claim(0);
    }

    private static NotificationOutboxClaim claim(int failureCount) {
        return new NotificationOutboxClaim(7L, 41L, 1,
                AttemptTrigger.INITIAL, "token", AT, failureCount);
    }

    /** 배치 안에서 <b>어느 건이</b> 되돌려졌는지 보려면 id·토큰이 달라야 한다. */
    private static NotificationOutboxClaim claim(long outboxId) {
        return new NotificationOutboxClaim(outboxId, 41L, 1,
                AttemptTrigger.INITIAL, "token-" + outboxId, AT, 0);
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

    /**
     * <b>배치당 한 번 읽어야 한다 — 건당이 아니라.</b>
     *
     * <p>워커가 건당 {@code findById} 를 부르면 발행 한 건에 커넥션을 <b>두 번</b> 빌리고,
     * 그 풀을 접수 요청과 공유한다. 실측(CY-926): 배치 조회로 바꾸니 <b>처리량이 전 구간
     * 20~30% 올랐다</b>(워커 8 기준 738 → 930~937건/s).
     *
     * <p>이 테스트가 없으면 누가 {@code publish} 안에서 다시 건별로 읽어도 아무도 모른다 —
     * 기능은 그대로 동작하고 <b>느려지기만</b> 한다.
     */
    @Test
    @org.junit.jupiter.api.DisplayName("한 회차의 알림을 배치로 한 번만 읽는다 — 건당이 아니다")
    void loadsTheBatchOnceInsteadOfPerClaim() {
        NotificationOutboxRelay relay = relayWith(LEASE, BATCH, Runnable::run, 64, 8);
        when(outboxes.claimBatch(LEASE, BATCH))
                .thenReturn(List.of(claim(1L), claim(2L), claim(3L)));
        when(notifications.findAllByIdIn(org.mockito.ArgumentMatchers.anyList()))
                .thenReturn(List.of(notification()));

        relay.poll();

        verify(notifications, org.mockito.Mockito.times(1))
                .findAllByIdIn(org.mockito.ArgumentMatchers.anyList());
        verify(notifications, never()).findById(org.mockito.ArgumentMatchers.anyLong());
    }

    /**
     * <b>같은 알림이 한 배치에 둘 들어올 수 있다.</b> 유일 키가
     * {@code (notification_id, attempt_seq)} 라, 자동 재시도로 회차가 다른 두 건이 함께
     * 집힌다. 중복을 그대로 넘기면 {@code IN} 절만 길어진다.
     */
    @Test
    @org.junit.jupiter.api.DisplayName("한 배치에 같은 알림이 둘이면 id 를 한 번만 넘긴다")
    void passesEachNotificationIdOnlyOnce() {
        NotificationOutboxRelay relay = relayWith(LEASE, BATCH, Runnable::run, 64, 8);
        when(outboxes.claimBatch(LEASE, BATCH))
                .thenReturn(List.of(claim(1L), claim(2L)));
        when(notifications.findAllByIdIn(org.mockito.ArgumentMatchers.anyList()))
                .thenReturn(List.of(notification()));

        relay.poll();

        org.mockito.ArgumentCaptor<java.util.Collection<Long>> ids =
                org.mockito.ArgumentCaptor.forClass(java.util.Collection.class);
        verify(notifications).findAllByIdIn(ids.capture());
        assertThat(ids.getValue())
                .as("두 claim 이 같은 알림을 가리킨다")
                .containsExactly(41L);
    }

    /**
     * <b>배치 조회 하나가 실패하면 집은 전부가 묶인다.</b>
     *
     * <p>건별 조회일 때는 한 건만 lease 만료를 기다렸다. 배치로 옮기면서 <b>한 번의 실패가
     * 최대 64건을 기본 30초 동안 {@code IN_PROGRESS} 로 붙잡는</b> 모양이 됐다 — 이 회차가
     * 통째로 멈추고, 그동안 아무도 그 행들을 못 집는다. 리뷰가 잡았다.
     *
     * <p>되돌린 뒤 <b>예외를 다시 던진다.</b> 삼키면 조회가 실패했다는 사실이 아무 데도
     * 안 남는다 — 아래 거부 경로와 같은 판단이다.
     */
    @Test
    @org.junit.jupiter.api.DisplayName("배치 조회가 실패하면 집은 전부를 되돌리고 다시 던진다")
    void aFailedBatchLoadReleasesEveryClaimItHadTaken() {
        NotificationOutboxRelay relay = relayWith(LEASE, BATCH, Runnable::run, 64, 8);
        when(outboxes.claimBatch(LEASE, BATCH))
                .thenReturn(List.of(claim(1L), claim(2L), claim(3L)));
        RuntimeException lookupFailed = new IllegalStateException("커넥션 없음");
        when(notifications.findAllByIdIn(org.mockito.ArgumentMatchers.anyList()))
                .thenThrow(lookupFailed);

        assertThatThrownBy(relay::poll).isSameAs(lookupFailed);

        verify(outboxes).releaseClaim(1L, "token-1");
        verify(outboxes).releaseClaim(2L, "token-2");
        verify(outboxes).releaseClaim(3L, "token-3");
        assertThat(relay.inFlight())
                .as("아무것도 안 넘겼으므로 인플라이트가 남으면 안 된다")
                .isZero();
    }
}

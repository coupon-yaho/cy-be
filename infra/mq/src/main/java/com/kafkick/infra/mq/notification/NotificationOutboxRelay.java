package com.kafkick.infra.mq.notification;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;

public class NotificationOutboxRelay implements AutoCloseable {

    /**
     * 건당 발행에 넉넉히 잡는 시간. <b>측정값이 아니라 예산이다</b> — 배치가 lease 를 태워
     * 뒤쪽 행이 회수된다는 관계를 기동 시 검사하는 데만 쓴다.
     *
     * <p><b>이 값이 얼마여야 하는지는 안 쟀다.</b> 재려면 발행 지연을 계측해야 하고
     * 그것은 이 클래스가 할 일이 아니다. 100ms 를 고른 것은 <b>기본 구성이 여유 있게
     * 통과하면서, 인플라이트를 키우거나 워커를 줄이거나 lease 를 줄일 때는 걸리는</b>
     * 값이기 때문이다 — 그 셋이 정확히 위험한 경우다.
     */
    private static final int PER_ITEM_PUBLISH_BUDGET_MILLIS = 100;

    /** {@link #awaitDrain} 이 인플라이트를 다시 볼 때까지 쉬는 간격. */
    private static final long DRAIN_POLL_NANOS = 1_000_000L;

    /**
     * lease 상한. <b>저장소 어댑터와 같은 365일</b>이다 — 여기서 더 좁히면 저장소가 받는
     * 설정을 릴레이가 먼저 거부하게 된다.
     *
     * <p><b>상한만 같고 아래쪽은 어댑터가 더 좁다.</b> 어댑터의 {@code durationSeconds} 는
     * <b>1초 이상의 정수 초</b>만 받으므로 {@code 500ms} 나 {@code 1500ms} 는 거기서 죽는다.
     * 그 검사를 여기로 옮기지 않는 것은 <b>단위 제약이 저장 방식에서 오는 것</b>이라
     * 어댑터가 주인이기 때문이다 — 두 벌로 두면 갈린다.
     *
     * <p><b>이 상한이 기동을 막는다. 그것이 목적이다.</b> 상한이 없으면 큰 값에서
     * {@link Duration#toMillis()} 가 {@code ArithmeticException} 으로 죽는데, 그 예외는
     * <b>무엇이 틀렸는지 말하지 않는다.</b> 어차피 기동이 안 될 것이라면 원인을 말하고
     * 죽는 편이 낫다 — {@code ExpireJobConfig} 가 {@code chunk-size} 에 하는 것과 같다.
     */
    private static final Duration MAX_LEASE = Duration.ofDays(365);

    private final NotificationOutboxRepository outboxes;
    private final NotificationRepository notifications;
    private final NotificationRequestedEventPublisher publisher;


    private final Duration lease;
    private final int claimBatchSize;
    private final FullJitterBackOff backOff;
    private final Clock clock;
    private final Executor workers;

    /**
     * 동시에 워커 풀에 물려 둘 수 있는 최대 건수. <b>백프레셔의 기준선이다.</b>
     *
     * <p>워커 수와 다르다 — 이보다 워커가 적으면 나머지는 풀의 큐에서 기다린다.
     * 그 대기까지 lease 를 태우므로 둘의 관계를 생성자가 검사한다.
     */
    private final int maxInFlight;

    /** 풀이 동시에 실행하는 작업 수. lease 검사가 <b>파도 깊이</b>를 이것으로 나눈다. */
    private final int workerCount;

    /**
     * 지금 워커 풀에 물려 둔 건수. 게이지가 이 값을 읽고, 백프레셔가 이 값으로 판단하고,
     * 종료 시 {@link #awaitDrain} 이 이 값이 0 이 되기를 기다린다.
     */
    private final AtomicInteger inFlight = new AtomicInteger();

    /**
     * {@link #close()} 가 세운다. <b>세워진 뒤의 {@link #poll()} 은 아무것도 집지 않는다</b> —
     * 안 그러면 배수하는 동안 스케줄러가 계속 새로 집어 영영 안 끝난다.
     */
    private volatile boolean stopping;

    /**
     * @throws NullPointerException 인자가 {@code null} 일 때 —
     *         {@link java.util.Objects#requireNonNull} 이 던진다
     * @throws IllegalArgumentException 아래 셋 중 하나일 때. <b>전부 빈 생성에서 터지므로
     *         기동이 거부된다</b> — 잘못 설정된 릴레이가 조용히 도는 것보다 낫다
     *         <ul>
     *           <li>{@code lease} 가 0·음수이거나 365일을 넘을 때</li>
     *           <li>{@code claimBatchSize}·{@code maxInFlight}·{@code workerCount} 중
     *               1 미만인 것이 있을 때</li>
     *           <li>{@code ceil(maxInFlight / workerCount) × 건당 발행 예산} 이
     *               {@code lease} 이상일 때 — 마지막 파도의 행이 처리 전에 회수되어
     *               <b>중복 발행</b>이 되는 구성이다</li>
     *         </ul>
     */
    public NotificationOutboxRelay(NotificationOutboxRepository outboxes,
            NotificationRepository notifications,
            NotificationRequestedEventPublisher publisher,
            Duration lease,
            int claimBatchSize,
            Executor workers,
            int maxInFlight,
            int workerCount,
            FullJitterBackOff backOff,
            Clock clock) {
        this.outboxes = Objects.requireNonNull(outboxes, "outboxes");
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.lease = Objects.requireNonNull(lease, "lease");
        if (claimBatchSize < 1) {
            throw new IllegalArgumentException(
                    "claimBatchSize 는 1 이상이어야 합니다. 0 이면 릴레이가 아무것도 집지 않고 "
                            + "조용히 정상으로 보입니다. 받은 값=" + claimBatchSize);
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException(
                    "maxInFlight 는 1 이상이어야 합니다. 0 이면 백프레셔가 항상 걸려 릴레이가 "
                            + "아무것도 집지 않고 조용히 정상으로 보입니다. 받은 값=" + maxInFlight);
        }
        if (workerCount < 1) {
            throw new IllegalArgumentException(
                    "workerCount 는 1 이상이어야 합니다. 받은 값=" + workerCount);
        }
        // **한 회차에 집은 것이 한 lease 를 공유한다.** 집는 순간 전부 claimed_at 이 찍히는데
        // 워커는 유한하므로, 워커 수를 넘는 만큼은 **큐에서 기다리는 동안 lease 를 태운다.**
        // 마지막 파도가 lease 를 넘으면 **아직 처리도 안 한 행이 회수되어 남이 다시 발행한다.**
        //
        // 파도 깊이는 ceil(maxInFlight / workerCount) 다 — claimBatchSize 가 아니다.
        // 한 회차에 집는 수는 아래 poll() 이 남은 인플라이트 여유로 잘라서 maxInFlight 를
        // 절대 넘지 않기 때문이다. CY-902 때는 워커가 하나뿐이라 깊이가 곧 배치 크기였고,
        // 그 시절 식(claimBatchSize × 예산)을 그대로 두면 **동시성을 넣고도 예전만큼만
        // 허용하게 된다.**
        //
        // lease 를 먼저 거른다. 상한이 없으면 아래 toMillis() 가 ArithmeticException 으로
        // 터져 **알림 릴레이 설정 하나가 접수 API 기동을 막는다.**
        if (lease.isNegative() || lease.isZero() || lease.compareTo(MAX_LEASE) > 0) {
            throw new IllegalArgumentException(
                    "lease 는 양수이고 " + MAX_LEASE + " 이하여야 합니다. 상한은 저장소 "
                            + "어댑터와 같습니다(아래쪽은 어댑터가 정수 초만 받아 더 좁습니다). "
                            + "받은 값=" + lease);
        }
        int waves = (maxInFlight + workerCount - 1) / workerCount;
        long budgetMillis = waves * (long) PER_ITEM_PUBLISH_BUDGET_MILLIS;
        if (budgetMillis >= lease.toMillis()) {
            throw new IllegalArgumentException(
                    "ceil(maxInFlight / workerCount) × 건당 발행 예산("
                            + PER_ITEM_PUBLISH_BUDGET_MILLIS + "ms) 이 lease 이상입니다. "
                            + "마지막 파도의 행이 처리 전에 회수되어 다른 워커가 같은 이벤트를 "
                            + "다시 발행합니다. maxInFlight=" + maxInFlight
                            + " workerCount=" + workerCount + " 파도=" + waves
                            + " 예산합=" + budgetMillis + "ms lease=" + lease.toMillis() + "ms");
        }
        this.claimBatchSize = claimBatchSize;
        this.workers = Objects.requireNonNull(workers, "workers");
        this.maxInFlight = maxInFlight;
        this.workerCount = workerCount;
        this.backOff = Objects.requireNonNull(backOff, "backOff");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * <b>다음 시도까지의 지연.</b> 클레임의 {@code failureCount} 는 <b>이번 실패를 세기 전</b>
     * 값이므로 {@code +1} 이 이번이 몇 번째 재시도인지다.
     *
     * <p>두 실패 경로가 같은 계산을 쓴다 — 발행 대상이 사라진 경우와 발행이 던진 경우.
     * 한쪽만 지터를 주면 나머지가 다시 뭉친다.
     */
    private Duration retryDelay(NotificationOutboxClaim claim) {
        return backOff.nextDelay(claim.failureCount() + 1);
    }

    /**
     * 한 회차. 인플라이트 여유만큼 집어 <b>워커 풀에 뿌리고 즉시 반환한다.</b>
     *
     * <p><b>이 메서드는 발행을 기다리지 않는다.</b> 스케줄러 스레드에서 블로킹하면 같은
     * 스케줄러에 걸린 다른 작업이 함께 멈춘다 — 풀 크기가 1이면 전부 멈춘다.
     *
     * <h2>백프레셔</h2>
     *
     * <p>집는 수를 <b>남은 인플라이트 여유</b>로 자른다. 여유가 없으면 <b>아무것도 집지 않고
     * 이번 회차를 건너뛴다.</b> 상한 없이 뿌리면 클레임이 처리 속도를 앞질러
     * {@code IN_PROGRESS} 가 쌓이고, lease 가 만료되어 <b>회수 → 재클레임 → 다시 만료</b>가
     * 도는 상태가 된다 — 일은 안 늘고 DB 쓰기만 는다.
     *
     * <p><b>여유는 원자적으로 잡지 않는다.</b> 이 메서드를 부르는 것은 {@code @Scheduled}
     * 스레드 하나뿐이라 회차끼리 겹치지 않고({@code fixedDelay} 는 이전 회차가 끝나야
     * 다음이 돈다), 워커가 동시에 줄이는 쪽으로만 움직이므로 실제 인플라이트는 여기서 잰
     * 여유보다 항상 작거나 같다. <b>넘칠 수 없는 방향의 경쟁이다.</b>
     *
     * @return 워커 풀에 넘긴 건수. <b>발행한 건수가 아니다</b> — 성공 여부는 워커가
     *         {@code markPublished}·{@code markFailed} 로 남긴다. 0 이면 집을 것이 없었거나
     *         백프레셔로 건너뛰었거나 종료 중이다
     */
    public int poll() {
        if (stopping) {
            return 0;
        }
        int capacity = maxInFlight - inFlight.get();
        if (capacity <= 0) {
            return 0;
        }
        List<NotificationOutboxClaim> claims =
                outboxes.claimBatch(lease, Math.min(claimBatchSize, capacity));
        for (NotificationOutboxClaim claim : claims) {
            // **먼저 올리고 넘긴다.** 워커가 먼저 돌아 내리는 것을 막으려는 것이 아니라,
            // execute() 가 거부(RejectedExecutionException)할 때 올린 것을 되돌리기 위해서다.
            inFlight.incrementAndGet();
            try {
                workers.execute(() -> {
                    try {
                        publish(claim);
                    } finally {
                        inFlight.decrementAndGet();
                    }
                });
            } catch (RuntimeException rejected) {
                inFlight.decrementAndGet();
                // 집어 놓고 못 돌린 것은 lease 만료로 회수된다. 여기서 markFailed 를 부르면
                // **거부가 곧 실패 1회로 세어져** failure_count 가 실제 발행 실패 없이 오른다.
                throw rejected;
            }
        }
        return claims.size();
    }

    /** 지금 워커 풀에 물려 둔 건수. 게이지가 읽는다. */
    public int inFlight() {
        return inFlight.get();
    }

    /**
     * <b>더 집지 않고, 이미 집은 것이 끝나기를 기다린다.</b>
     *
     * <p>안 기다리면 인플라이트가 {@code IN_PROGRESS} 로 남고, 그 행은 <b>lease 가 만료될
     * 때까지</b> 아무도 못 집는다 — 재기동 직후 그만큼이 지연된다.
     *
     * <p>이 메서드는 <b>워커 풀을 닫지 않는다.</b> 풀의 수명은 그것을 만든 쪽(설정)이
     * 가지고, 여기서 닫으면 같은 풀을 쓰는 다른 쪽이 있을 때 조용히 망가진다.
     *
     * @return 다 빠졌으면 {@code true}, 시간 안에 못 빠졌으면 {@code false}
     */
    public boolean awaitDrain(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        stopping = true;
        long deadline = System.nanoTime() + timeout.toNanos();
        while (inFlight.get() > 0) {
            if (System.nanoTime() - deadline >= 0) {
                return false;
            }
            LockSupport.parkNanos(DRAIN_POLL_NANOS);
        }
        return true;
    }

    /**
     * 기본 배수 시간. <b>파도 하나가 예산 안에 끝난다는 가정과 같은 값</b>이라, 이것을 넘겨
     * 못 빠졌다면 발행이 예산보다 훨씬 느리다는 뜻이다.
     */
    private Duration defaultDrainTimeout() {
        int waves = (maxInFlight + workerCount - 1) / workerCount;
        return Duration.ofMillis(waves * (long) PER_ITEM_PUBLISH_BUDGET_MILLIS);
    }

    @Override
    public void close() {
        awaitDrain(defaultDrainTimeout());
    }

    private boolean publish(NotificationOutboxClaim claim) {
        Optional<Notification> found = notifications.findById(claim.notificationId());
        if (found.isEmpty()) {
            // 발행 대상이 사라졌다. 지연은 발행 실패와 같은 계산을 쓴다 — 한쪽만
            // 지터를 주면 나머지가 다시 뭉친다.
            outboxes.markFailed(claim.outboxId(), claim.claimToken(), retryDelay(claim));
            return false;
        }
        Notification notification = found.orElseThrow();

        NotificationRequestedEvent event = new NotificationRequestedEvent(
                notification.id(), notification.memberId(), notification.couponId(),
                claim.attemptSeq(), claim.trigger(), claim.requestedAt());
        try {
            publisher.publish(event);
            outboxes.markPublished(claim.outboxId(), claim.claimToken(), clock.instant());
            return true;
        } catch (RuntimeException failure) {
            outboxes.markFailed(claim.outboxId(), claim.claimToken(), retryDelay(claim));
            return false;
        }
    }
}

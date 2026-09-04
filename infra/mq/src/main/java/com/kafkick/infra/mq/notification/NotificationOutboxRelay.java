package com.kafkick.infra.mq.notification;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.event.NotificationRequestedEvent;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;

public class NotificationOutboxRelay {

    private final NotificationOutboxRepository outboxes;
    private final NotificationRepository notifications;
    private final NotificationRequestedEventPublisher publisher;
    /**
     * 건당 발행에 넉넉히 잡는 시간. <b>측정값이 아니라 예산이다</b> — 이 값을 넘기면
     * 배치가 lease 를 태워 뒤쪽 행이 회수된다는 관계를 기동 시 검사하는 데만 쓴다.
     *
     * <p>로컬 Kafka 발행은 보통 한 자릿수 ms 다. 100ms 는 그 열 배 이상이라 평시에는
     * 안 걸리고, <b>배치를 크게 잡거나 lease 를 줄일 때</b> 걸린다 — 그 두 경우가 정확히
     * 위험한 경우다.
     */
    private static final int PER_ITEM_PUBLISH_BUDGET_MILLIS = 100;

    private final Duration lease;
    private final int claimBatchSize;
    private final FullJitterBackOff backOff;
    private final Clock clock;

    /**
     * @throws IllegalArgumentException {@code claimBatchSize} 가 1 미만이거나,
     *         {@code claimBatchSize × 건당 발행 예산} 이 {@code lease} 이상일 때 —
     *         후자는 배치 뒤쪽 행이 처리 전에 회수되어 <b>중복 발행</b>이 되는 구성이다
     */
    public NotificationOutboxRelay(NotificationOutboxRepository outboxes,
            NotificationRepository notifications,
            NotificationRequestedEventPublisher publisher,
            Duration lease,
            int claimBatchSize,
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
        // **배치 전체가 한 lease 를 공유한다.** 집는 순간 전부 claimed_at 이 찍히는데
        // 발행은 차례로 도므로, 뒤쪽 행은 앞쪽이 끝나기를 기다리는 동안 lease 를 태운다.
        // 그 합이 lease 를 넘으면 **아직 처리도 안 한 행이 회수되어 남이 다시 발행한다.**
        long budgetMillis = claimBatchSize * (long) PER_ITEM_PUBLISH_BUDGET_MILLIS;
        if (budgetMillis >= lease.toMillis()) {
            throw new IllegalArgumentException(
                    "claimBatchSize × 건당 발행 예산(" + PER_ITEM_PUBLISH_BUDGET_MILLIS
                            + "ms) 이 lease 이상입니다. 배치 뒤쪽 행이 처리 전에 회수되어 "
                            + "다른 워커가 같은 이벤트를 다시 발행합니다. "
                            + "claimBatchSize=" + claimBatchSize + " 예산합=" + budgetMillis
                            + "ms lease=" + lease.toMillis() + "ms");
        }
        this.claimBatchSize = claimBatchSize;
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
     * 한 번에 여러 건 집어 <b>차례로</b> 발행한다.
     *
     * <p><b>아직 차례로다.</b> 배치로 집는 것과 동시에 보내는 것은 다른 문제이고,
     * 워커 풀은 CY-906(#195)이 붙인다. 여기까지만으로도 선점 경합이 사라지고 왕복이 줄지만,
     * 발행 자체가 느린 대상(예: 외부 HTTP)에서는 <b>동시성이 없으면 처리량이 안 는다.</b>
     *
     * @return 실제로 발행한 건수. 0 이면 집을 것이 없었거나 전부 실패했다
     */
    public int poll() {
        int published = 0;
        for (NotificationOutboxClaim claim : outboxes.claimBatch(lease, claimBatchSize)) {
            if (publish(claim)) {
                published++;
            }
        }
        return published;
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

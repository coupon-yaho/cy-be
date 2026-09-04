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
    private final Duration lease;
    private final int claimBatchSize;
    private final FullJitterBackOff backOff;
    private final Clock clock;

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

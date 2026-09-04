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

    /**
     * 건당 발행에 넉넉히 잡는 시간. <b>측정값이 아니라 예산이다</b> — 이 값을 넘기면
     * 배치가 lease 를 태워 뒤쪽 행이 회수된다는 관계를 기동 시 검사하는 데만 쓴다.
     *
     * <p><b>이 값이 얼마여야 하는지는 안 쟀다.</b> 재려면 발행 지연을 계측해야 하고
     * 그것은 이 클래스가 할 일이 아니다. 100ms 를 고른 것은 <b>기본 구성(배치 64 · lease 30s)이
     * 여유 있게 통과하면서, 배치를 키우거나 lease 를 줄일 때는 걸리는</b> 값이기 때문이다 —
     * 그 두 경우가 정확히 위험한 경우다.
     */
    private static final int PER_ITEM_PUBLISH_BUDGET_MILLIS = 100;

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

    /**
     * @throws NullPointerException 인자가 {@code null} 일 때 —
     *         {@link java.util.Objects#requireNonNull} 이 던진다
     * @throws IllegalArgumentException 아래 셋 중 하나일 때. <b>전부 빈 생성에서 터지므로
     *         기동이 거부된다</b> — 잘못 설정된 릴레이가 조용히 도는 것보다 낫다
     *         <ul>
     *           <li>{@code lease} 가 0·음수이거나 365일을 넘을 때</li>
     *           <li>{@code claimBatchSize} 가 1 미만일 때</li>
     *           <li>{@code claimBatchSize × 건당 발행 예산} 이 {@code lease} 이상일 때 —
     *               배치 뒤쪽 행이 처리 전에 회수되어 <b>중복 발행</b>이 되는 구성이다</li>
     *         </ul>
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
        // lease 를 먼저 거른다. 상한이 없으면 아래 toMillis() 가 ArithmeticException 으로
        // 터져 **알림 릴레이 설정 하나가 접수 API 기동을 막는다.**
        if (lease.isNegative() || lease.isZero() || lease.compareTo(MAX_LEASE) > 0) {
            throw new IllegalArgumentException(
                    "lease 는 양수이고 " + MAX_LEASE + " 이하여야 합니다. 상한은 저장소 "
                            + "어댑터와 같습니다(아래쪽은 어댑터가 정수 초만 받아 더 좁습니다). "
                            + "받은 값=" + lease);
        }
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

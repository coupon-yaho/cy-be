package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationStatus;

/**
 * <b>멱등 키가 "같은 논리적 발송" 을 정확히 가리키는지.</b>
 *
 * <p>outbox 도 Kafka 도 at-least-once 라 같은 알림이 여러 번 도착한다. 그것을 합치는 것은
 * <b>받는 쪽</b>이고, 우리가 할 일은 <b>같은 발송에 같은 키를 주는 것</b>뿐이다.
 *
 * <p><b>틀리는 방향이 둘이고 둘 다 조용하다.</b>
 *
 * <ul>
 *   <li><b>키가 너무 자주 바뀌면</b> — 자동 재시도마다 다른 키가 가서, 받는 쪽이 첫 요청을
 *       처리한 뒤 <b>응답만 타임아웃된</b> 경우 두 번 발송된다. 사용자가 알림을 두 번 받는다</li>
 *   <li><b>키가 너무 오래 같으면</b> — 사람이 재발송한 것을 받는 쪽이 중복으로 버린다.
 *       <b>안 온다고 다시 보낸 것이 안 간다</b></li>
 * </ul>
 *
 * <p>그 경계가 {@code baseAttemptSeq} 다 — 자동 재시도에서는 안 변하고, 사람이 재발송하면
 * 올라간다. 이 테스트가 그 두 방향을 함께 잡는다.
 */
class NotificationIdempotencyKeyTest {

    private static final Instant AT = Instant.parse("2026-09-05T00:00:00Z");

    private static Notification notification() {
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 1, 0, null, "member:20", "coupon-issued:100",
                AT, AT, null, null);
    }

    private static NotificationDeliveryDecision decision(int baseAttemptSeq, int attemptSeq) {
        return NotificationDeliveryDecision.send(
                notification(), baseAttemptSeq, attemptSeq, AttemptTrigger.INITIAL);
    }

    /**
     * <b>자동 재시도는 같은 발송이다.</b> {@code attemptSeq} 만 오르고 키는 그대로여야
     * 받는 쪽이 합칠 수 있다.
     */
    @Test
    @DisplayName("자동 재시도에서 키가 안 바뀐다 — 안 그러면 사용자가 두 번 받는다")
    void staysTheSameAcrossAutomaticRetries() {
        assertThat(decision(1, 1).idempotencyKey())
                .isEqualTo(decision(1, 2).idempotencyKey())
                .isEqualTo(decision(1, 3).idempotencyKey());
    }

    /**
     * <b>사람이 재발송하면 다른 발송이다.</b> 안 왔다고 다시 보낸 것인데 같은 키면
     * 받는 쪽이 중복으로 버린다.
     */
    @Test
    @DisplayName("사람이 재발송하면 키가 바뀐다 — 안 그러면 재발송이 버려진다")
    void changesWhenAHumanResends() {
        assertThat(decision(1, 1).idempotencyKey())
                .isNotEqualTo(decision(2, 2).idempotencyKey());
    }

    /** 다른 알림은 당연히 다른 키다. 같으면 서로를 지운다. */
    @Test
    @DisplayName("알림이 다르면 키가 다르다")
    void differsBetweenNotifications() {
        Notification other = new Notification(42L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.PENDING, 1, 0, null,
                "member:20", "coupon-issued:100", AT, AT, null, null);

        assertThat(decision(1, 1).idempotencyKey())
                .isNotEqualTo(NotificationDeliveryDecision
                        .send(other, 1, 1, AttemptTrigger.INITIAL).idempotencyKey());
    }
}

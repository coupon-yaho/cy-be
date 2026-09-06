package com.kafkick.core.notification;

import java.util.Objects;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;

public record NotificationDeliveryDecision(Action action, Notification notification,
        int baseAttemptSeq, int attemptSeq, AttemptTrigger trigger) {
    public enum Action { SEND, ACKNOWLEDGE }

    public NotificationDeliveryDecision {
        Objects.requireNonNull(action, "action");
        if (action == Action.SEND) {
            Objects.requireNonNull(notification, "notification");
            Objects.requireNonNull(trigger, "trigger");
            if (baseAttemptSeq < 1 || attemptSeq < baseAttemptSeq) {
                throw new IllegalArgumentException("발송 회차는 양수이고 시작 회차보다 작을 수 없습니다.");
            }
        }
    }

    public static NotificationDeliveryDecision send(Notification notification, int baseAttemptSeq,
            int attemptSeq, AttemptTrigger trigger) {
        return new NotificationDeliveryDecision(Action.SEND, notification,
                baseAttemptSeq, attemptSeq, trigger);
    }

    /**
     * 받는 쪽이 중복을 합칠 수 있게 하는 키.
     *
     * <p><b>{@link #baseAttemptSeq} 를 쓴다 — {@code attemptSeq} 가 아니다.</b>
     * 자동 재시도는 {@code attemptSeq} 를 올리지만 <b>같은 논리적 발송</b>이라, 그것으로
     * 키를 만들면 재시도마다 키가 바뀌어 받는 쪽이 못 합친다: 첫 요청이 처리된 뒤 응답만
     * 타임아웃된 경우 <b>두 번 발송된다.</b>
     *
     * <p>반대로 사람이 재발송하면 {@code baseAttemptSeq} 가 올라간다 — 그것은 <b>실제로
     * 다시 보내야 하는 건</b>이라 합쳐지면 안 된다. 그 경계가 정확히 이 값이다.
     */
    public String idempotencyKey() {
        return notification.id() + ":" + baseAttemptSeq;
    }

    public static NotificationDeliveryDecision acknowledge() {
        return new NotificationDeliveryDecision(Action.ACKNOWLEDGE, null, 0, 0, null);
    }
}

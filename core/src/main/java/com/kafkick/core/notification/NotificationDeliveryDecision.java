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

    public static NotificationDeliveryDecision acknowledge() {
        return new NotificationDeliveryDecision(Action.ACKNOWLEDGE, null, 0, 0, null);
    }
}

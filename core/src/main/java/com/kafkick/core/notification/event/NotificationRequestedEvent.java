package com.kafkick.core.notification.event;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.notification.domain.AttemptTrigger;

public record NotificationRequestedEvent(Long notificationId, Long memberId, Long couponId,
        int attemptSeq, AttemptTrigger trigger, Instant requestedAt) {
    public NotificationRequestedEvent {
        if (notificationId == null || notificationId <= 0 || memberId == null || memberId <= 0
                || couponId == null || couponId <= 0 || attemptSeq < 1) {
            throw new IllegalArgumentException("이벤트 식별자와 시도 번호는 양수여야 합니다.");
        }
        if (trigger != AttemptTrigger.INITIAL && trigger != AttemptTrigger.MANUAL) {
            throw new IllegalArgumentException("요청 이벤트 trigger는 INITIAL 또는 MANUAL이어야 합니다.");
        }
        Objects.requireNonNull(requestedAt, "requestedAt");
    }
}

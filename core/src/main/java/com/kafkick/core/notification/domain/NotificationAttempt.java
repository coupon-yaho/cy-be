package com.kafkick.core.notification.domain;

import java.time.Instant;
import java.util.Objects;

public record NotificationAttempt(Long id, Long notificationId, int attemptSeq,
        AttemptTrigger trigger, AttemptResult result, NotifyFailureReason failureReason,
        Instant startedAt, Instant finishedAt, Instant createdAt) {
    public NotificationAttempt {
        if (notificationId == null || notificationId <= 0 || attemptSeq < 1) {
            throw new IllegalArgumentException("알림 ID와 시도 번호는 양수여야 합니다.");
        }
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(finishedAt, "finishedAt");
        Objects.requireNonNull(createdAt, "createdAt");
        if ((result == AttemptResult.FAILED) != (failureReason != null)) {
            throw new IllegalArgumentException("실패 결과와 실패 사유는 같이 존재해야 합니다.");
        }
    }
}

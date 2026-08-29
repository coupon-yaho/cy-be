package com.kafkick.core.notification;

import java.time.Instant;
import java.util.Objects;

/**
 * 비동기 수동 재발송의 접수 결과입니다.
 *
 * @param notificationId 접수된 양수 알림 식별자
 * @param attemptSeq 새로 선점한 1 이상의 시도 번호
 * @param requestedAt null이 아닌 접수 시각
 */
public record NotificationResendResult(Long notificationId, int attemptSeq, Instant requestedAt) {
    /**
     * @throws IllegalArgumentException 알림 식별자 또는 시도 번호가 양수가 아닌 경우
     * @throws NullPointerException 접수 시각이 null인 경우
     */
    public NotificationResendResult {
        if (notificationId == null || notificationId <= 0 || attemptSeq < 1) {
            throw new IllegalArgumentException("재발송 식별자와 시도 번호는 양수여야 합니다.");
        }
        Objects.requireNonNull(requestedAt, "requestedAt");
    }
}

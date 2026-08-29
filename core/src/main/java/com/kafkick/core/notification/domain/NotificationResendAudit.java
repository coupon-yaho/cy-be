package com.kafkick.core.notification.domain;

import java.time.Instant;
import java.util.Objects;

public record NotificationResendAudit(Long id, Long notificationId, Integer attemptSeq,
        Long requestedBy, Instant requestedAt, boolean accepted, String rejectCode,
        Instant createdAt) {
    public NotificationResendAudit {
        if (notificationId == null || notificationId <= 0 || requestedBy == null || requestedBy <= 0) {
            throw new IllegalArgumentException("감사 식별자와 시도 번호는 양수여야 합니다.");
        }
        Objects.requireNonNull(requestedAt, "requestedAt");
        Objects.requireNonNull(createdAt, "createdAt");
        if (accepted == (rejectCode != null)) {
            throw new IllegalArgumentException("접수 여부와 거부 코드가 일치해야 합니다.");
        }
        if ((accepted && (attemptSeq == null || attemptSeq < 1))
                || (!accepted && attemptSeq != null)) {
            throw new IllegalArgumentException("접수된 요청만 시도 번호를 가져야 합니다.");
        }
    }
}

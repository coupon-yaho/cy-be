package com.kafkick.core.notification.domain;

import java.time.Instant;
import java.util.Objects;
import com.kafkick.core.notification.NotificationErrorCode;
import com.kafkick.core.support.exception.BusinessException;

public record Notification(
        Long id,
        Long couponId,
        Long memberId,
        Long issuanceId,
        String channel,
        NotificationStatus status,
        int attemptCount,
        int resendCount,
        NotifyFailureReason lastFailureReason,
        String recipientContact,
        String messageBody,
        Instant createdAt,
        Instant updatedAt,
        Instant sentAt,
        Instant failedAt
) {
    public static final String DEFAULT_CHANNEL = "DEFAULT";
    public static final int MAX_RESEND_COUNT = 3;
    public static final int MAX_RECIPIENT_CHARACTERS = 255;
    public static final int MAX_MESSAGE_CHARACTERS = 500;

    public Notification {
        requirePositive(couponId, "couponId");
        requirePositive(memberId, "memberId");
        requirePositive(issuanceId, "issuanceId");
        if (!DEFAULT_CHANNEL.equals(channel)) {
            throw new IllegalArgumentException("현재 알림 채널은 DEFAULT만 지원합니다.");
        }
        Objects.requireNonNull(status, "status");
        requireText(recipientContact, MAX_RECIPIENT_CHARACTERS, "recipientContact");
        requireText(messageBody, MAX_MESSAGE_CHARACTERS, "messageBody");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        if (attemptCount < 0 || resendCount < 0 || resendCount > MAX_RESEND_COUNT) {
            throw new IllegalArgumentException("시도 횟수는 허용 범위여야 합니다.");
        }
        if ((status == NotificationStatus.SENT) != (sentAt != null)) {
            throw new IllegalArgumentException("SENT 상태와 발송 시각은 같이 존재해야 합니다.");
        }
        boolean failed = status == NotificationStatus.FAILED || status == NotificationStatus.DEAD;
        if (failed != (lastFailureReason != null)) {
            throw new IllegalArgumentException("실패 상태와 실패 사유는 같이 존재해야 합니다.");
        }
        if (failed && failedAt == null) {
            throw new IllegalArgumentException("실패 상태에는 실패 시각이 필요합니다.");
        }
    }

    public static Notification pending(Long couponId, Long memberId, Long issuanceId,
            String recipientContact, String messageBody, Instant createdAt) {
        return new Notification(null, couponId, memberId, issuanceId, DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 0, 0, null, recipientContact, messageBody,
                createdAt, createdAt, null, null);
    }

    public Notification startSending(AttemptTrigger trigger, Instant at) {
        Objects.requireNonNull(trigger, "trigger");
        if (trigger == AttemptTrigger.MANUAL && resendCount >= MAX_RESEND_COUNT) {
            throw new NotificationResendLimitExceededException(id);
        }
        boolean allowed = (status == NotificationStatus.PENDING && trigger == AttemptTrigger.INITIAL)
                || (status == NotificationStatus.FAILED
                    && (trigger == AttemptTrigger.AUTO || trigger == AttemptTrigger.MANUAL))
                || (status == NotificationStatus.DEAD && trigger == AttemptTrigger.MANUAL);
        if (!allowed) {
            throw new NotificationInvalidTransitionException(status, NotificationStatus.SENDING);
        }
        return copy(NotificationStatus.SENDING, attemptCount + 1,
                resendCount + (trigger == AttemptTrigger.MANUAL ? 1 : 0), null, at, null, failedAt);
    }

    public Notification resumeSending(int attemptSeq, Instant at) {
        if (status != NotificationStatus.SENDING || attemptCount != attemptSeq) {
            throw new NotificationInvalidTransitionException(status, NotificationStatus.SENDING);
        }
        return copy(NotificationStatus.SENDING, attemptCount, resendCount, null,
                Objects.requireNonNull(at, "at"), null, failedAt);
    }

    public Notification markSent(Instant at) {
        requireStatus(NotificationStatus.SENDING, NotificationStatus.SENT);
        return copy(NotificationStatus.SENT, attemptCount, resendCount, null, at, at, null);
    }

    public Notification markFailed(NotifyFailureReason reason, Instant at) {
        requireStatus(NotificationStatus.SENDING, NotificationStatus.FAILED);
        return copy(NotificationStatus.FAILED, attemptCount, resendCount,
                Objects.requireNonNull(reason, "reason"), at, null, at);
    }

    public Notification markDead(NotifyFailureReason reason, Instant at) {
        if (status != NotificationStatus.SENDING && status != NotificationStatus.FAILED) {
            throw new NotificationInvalidTransitionException(status, NotificationStatus.DEAD);
        }
        return copy(NotificationStatus.DEAD, attemptCount, resendCount,
                Objects.requireNonNull(reason, "reason"), at, null, at);
    }

    private void requireStatus(NotificationStatus expected, NotificationStatus next) {
        if (status != expected) {
            throw new NotificationInvalidTransitionException(status, next);
        }
    }

    private Notification copy(NotificationStatus next, int attempts, int resends,
            NotifyFailureReason reason, Instant updated, Instant sent, Instant failed) {
        return new Notification(id, couponId, memberId, issuanceId, channel, next, attempts,
                resends, reason, recipientContact, messageBody, createdAt, updated, sent, failed);
    }

    private static void requirePositive(Long value, String name) {
        if (value == null || value <= 0) {
            throw new IllegalArgumentException(name + "는 0보다 커야 합니다.");
        }
    }

    private static void requireText(String value, int maxCharacters, String name) {
        if (value == null || value.isBlank()) {
            throw new BusinessException(NotificationErrorCode.PAYLOAD_REQUIRED,
                    name + "는 비어 있을 수 없습니다.");
        }
        if (value.length() > maxCharacters) {
            throw new BusinessException(NotificationErrorCode.PAYLOAD_TOO_LARGE,
                    name + "가 저장 가능한 길이를 초과했습니다.");
        }
    }

    @Override
    public String toString() {
        return "Notification[id=" + id + ", couponId=" + couponId + ", memberId=" + memberId
                + ", issuanceId=" + issuanceId + ", status=" + status + ", attemptCount="
                + attemptCount + ", resendCount=" + resendCount + "]";
    }
}

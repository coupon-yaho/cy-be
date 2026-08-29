package com.kafkick.core.notification;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationResendAudit;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.support.TimeProvider;

public class NotificationResendService {
    private static final Duration IDEMPOTENCY_WINDOW = Duration.ofMinutes(10);

    private final NotificationRepository notifications;
    private final NotificationOutboxRepository outboxes;
    private final NotificationResendAuditRepository audits;
    private final NotificationRejectedAuditWriter rejectedAudits;
    private final TimeProvider timeProvider;

    public NotificationResendService(NotificationRepository notifications,
            NotificationOutboxRepository outboxes,
            NotificationResendAuditRepository audits,
            NotificationRejectedAuditWriter rejectedAudits,
            TimeProvider timeProvider) {
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.outboxes = Objects.requireNonNull(outboxes, "outboxes");
        this.audits = Objects.requireNonNull(audits, "audits");
        this.rejectedAudits = Objects.requireNonNull(rejectedAudits, "rejectedAudits");
        this.timeProvider = Objects.requireNonNull(timeProvider, "timeProvider");
    }

    /**
     * 실패 또는 종결 알림의 수동 재발송을 원자적으로 접수합니다.
     *
     * <p>알림 미존재, FAILED·DEAD 외 상태, 3회 상한, 최근 accepted 감사 기준 10분 멱등 창,
     * 또는 status·attempt_count·resend_count CAS 패배는 거부 감사 기록 후
     * {@link NotificationResendRejectedException}을 발생시킵니다. accepted 감사와 MANUAL
     * outbox 저장이 실패하면 알림 선점도 같은 트랜잭션에서 롤백됩니다.
     *
     * @param notificationId 재발송할 양수 알림 식별자
     * @param requestedBy 요청한 양수 관리자 회원 식별자
     * @return 새 시도 번호와 접수 시각
     * @throws IllegalArgumentException 식별자 중 하나라도 양수가 아닌 경우
     * @throws NotificationResendRejectedException 재발송 정책 또는 CAS 선점으로 거부된 경우
     */
    @Transactional
    public NotificationResendResult resend(Long notificationId, Long requestedBy) {
        requirePositive(notificationId, "notificationId");
        requirePositive(requestedBy, "requestedBy");
        Instant requestedAt = timeProvider.instant();
        Notification current = notifications.findById(notificationId).orElse(null);
        if (current == null) {
            reject(notificationId, requestedBy, requestedAt, NotificationResendRejection.NOT_FOUND);
        }
        if (current.status() != NotificationStatus.FAILED
                && current.status() != NotificationStatus.DEAD) {
            reject(notificationId, requestedBy, requestedAt, NotificationResendRejection.CONFLICT);
        }
        if (current.resendCount() >= Notification.MAX_RESEND_COUNT) {
            reject(notificationId, requestedBy, requestedAt,
                    NotificationResendRejection.LIMIT_EXCEEDED);
        }
        boolean insideWindow = audits.findLatestAcceptedByNotificationId(notificationId)
                .map(audit -> audit.requestedAt().isAfter(requestedAt.minus(IDEMPOTENCY_WINDOW)))
                .orElse(false);
        if (insideWindow) {
            reject(notificationId, requestedBy, requestedAt, NotificationResendRejection.CONFLICT);
        }

        Notification sending = current.startSending(AttemptTrigger.MANUAL, requestedAt);
        boolean claimed = notifications.saveIfStatus(sending, current.status(),
                current.attemptCount(), current.resendCount());
        if (!claimed) {
            reject(notificationId, requestedBy, requestedAt, NotificationResendRejection.CONFLICT);
        }

        audits.save(new NotificationResendAudit(null, notificationId, sending.attemptCount(),
                requestedBy, requestedAt, true, null, requestedAt));
        outboxes.save(NotificationOutbox.pending(notificationId, sending.attemptCount(),
                AttemptTrigger.MANUAL, requestedAt));
        return new NotificationResendResult(notificationId, sending.attemptCount(), requestedAt);
    }

    private void reject(Long notificationId, Long requestedBy, Instant requestedAt,
            NotificationResendRejection rejection) {
        rejectedAudits.write(notificationId, requestedBy, requestedAt, rejection.code());
        throw new NotificationResendRejectedException(rejection);
    }

    private static void requirePositive(Long value, String name) {
        if (value == null || value <= 0) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
    }
}

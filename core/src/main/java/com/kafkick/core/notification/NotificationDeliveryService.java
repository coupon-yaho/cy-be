package com.kafkick.core.notification;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.notification.domain.AttemptResult;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationAttempt;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.notification.event.NotificationRequestedEvent;

@Service
public class NotificationDeliveryService {
    private static final int MAX_AUTOMATIC_RETRIES = 3;

    private final NotificationRepository notifications;
    private final NotificationAttemptRepository attempts;
    private final NotificationOutboxRepository outboxes;

    public NotificationDeliveryService(NotificationRepository notifications,
            NotificationAttemptRepository attempts,
            NotificationOutboxRepository outboxes) {
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.attempts = Objects.requireNonNull(attempts, "attempts");
        this.outboxes = Objects.requireNonNull(outboxes, "outboxes");
    }

    @Transactional
    public NotificationDeliveryDecision prepare(NotificationRequestedEvent event, Instant at) {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(at, "at");
        Notification current = notifications.findById(event.notificationId()).orElse(null);
        if (current == null || current.status() == NotificationStatus.SENT
                || current.status() == NotificationStatus.DEAD) {
            return NotificationDeliveryDecision.acknowledge();
        }
        if (!current.memberId().equals(event.memberId())
                || !current.couponId().equals(event.couponId())) {
            return NotificationDeliveryDecision.acknowledge();
        }

        List<NotificationAttempt> completed = attempts.findByNotificationId(current.id());
        if (current.status() == NotificationStatus.PENDING) {
            if (event.trigger() != AttemptTrigger.INITIAL || event.attemptSeq() != 1
                    || current.attemptCount() != 0 || !completed.isEmpty()) {
                return NotificationDeliveryDecision.acknowledge();
            }
            Notification sending = current.startSending(AttemptTrigger.INITIAL, at);
            return claim(current, sending, event.attemptSeq(), AttemptTrigger.INITIAL);
        }

        if (!belongsToLineage(event, current.attemptCount(), completed)) {
            return NotificationDeliveryDecision.acknowledge();
        }
        if (current.status() == NotificationStatus.SENDING) {
            boolean currentAttemptCompleted = completed.stream()
                    .anyMatch(attempt -> attempt.attemptSeq() == current.attemptCount());
            if (currentAttemptCompleted) {
                return NotificationDeliveryDecision.acknowledge();
            }
            AttemptTrigger currentTrigger = currentAttemptTrigger(current);
            if (currentTrigger == AttemptTrigger.MANUAL
                    && (event.attemptSeq() != current.attemptCount()
                        || event.trigger() != AttemptTrigger.MANUAL)) {
                // 이전 계보의 지연 이벤트가 현재 MANUAL 시도를 대신 발송하지 못하게 ACK 후 폐기한다.
                return NotificationDeliveryDecision.acknowledge();
            }
            return NotificationDeliveryDecision.send(current.resumeSending(current.attemptCount(), at),
                    event.attemptSeq(), current.attemptCount(), currentTrigger);
        }
        if (current.status() == NotificationStatus.FAILED
                && current.lastFailureReason() != null
                && current.lastFailureReason().retryable()
                && current.attemptCount() - event.attemptSeq() < MAX_AUTOMATIC_RETRIES) {
            Notification sending = current.startSending(AttemptTrigger.AUTO, at);
            return claim(current, sending, event.attemptSeq(), AttemptTrigger.AUTO);
        }
        return NotificationDeliveryDecision.acknowledge();
    }

    private NotificationDeliveryDecision claim(Notification current, Notification sending,
            int baseAttemptSeq, AttemptTrigger trigger) {
        boolean won = notifications.saveIfStatus(
                sending, current.status(), current.attemptCount(), current.resendCount());
        if (!won) {
            return NotificationDeliveryDecision.acknowledge();
        }
        return NotificationDeliveryDecision.send(
                sending, baseAttemptSeq, sending.attemptCount(), trigger);
    }

    private static boolean belongsToLineage(NotificationRequestedEvent event, int currentAttempt,
            List<NotificationAttempt> completed) {
        if (currentAttempt < event.attemptSeq()) {
            return false;
        }
        for (int sequence = event.attemptSeq(); sequence <= currentAttempt; sequence++) {
            int expected = sequence;
            NotificationAttempt attempt = completed.stream()
                    .filter(candidate -> candidate.attemptSeq() == expected)
                    .findFirst().orElse(null);
            if (sequence == currentAttempt && attempt == null) {
                return true;
            }
            if (attempt == null) {
                return false;
            }
            AttemptTrigger expectedTrigger = sequence == event.attemptSeq()
                    ? event.trigger() : AttemptTrigger.AUTO;
            if (attempt.trigger() != expectedTrigger) {
                return false;
            }
        }
        return true;
    }

    private AttemptTrigger currentAttemptTrigger(Notification current) {
        return outboxes.findTriggerByNotificationIdAndAttemptSeq(
                current.id(), current.attemptCount())
                .orElse(current.attemptCount() == 1
                        ? AttemptTrigger.INITIAL : AttemptTrigger.AUTO);
    }

    @Transactional
    public boolean completeSuccess(NotificationDeliveryDecision decision,
            Instant startedAt, Instant finishedAt) {
        requireSend(decision);
        NotificationAttempt attempt = new NotificationAttempt(null,
                decision.notification().id(), decision.attemptSeq(), decision.trigger(),
                AttemptResult.SUCCESS, null, startedAt, finishedAt, finishedAt);
        if (!attempts.saveIfAbsent(attempt)) {
            return false;
        }
        Notification sent = decision.notification().markSent(finishedAt);
        if (!notifications.saveIfStatus(sent, NotificationStatus.SENDING,
                decision.attemptSeq(), decision.notification().resendCount())) {
            throw new IllegalStateException("완료 attempt 승자가 알림 성공 상태를 확정하지 못했습니다.");
        }
        return true;
    }

    @Transactional
    public FailureOutcome completeFailure(NotificationDeliveryDecision decision,
            NotifyFailureReason reason, Instant startedAt, Instant finishedAt) {
        requireSend(decision);
        Objects.requireNonNull(reason, "reason");
        NotificationAttempt attempt = new NotificationAttempt(null,
                decision.notification().id(), decision.attemptSeq(), decision.trigger(),
                AttemptResult.FAILED, reason, startedAt, finishedAt, finishedAt);
        if (!attempts.saveIfAbsent(attempt)) {
            return FailureOutcome.DUPLICATE;
        }
        boolean terminal = !reason.retryable()
                || decision.attemptSeq() - decision.baseAttemptSeq() >= MAX_AUTOMATIC_RETRIES;
        Notification next = terminal
                ? decision.notification().markDead(reason, finishedAt)
                : decision.notification().markFailed(reason, finishedAt);
        if (!notifications.saveIfStatus(next, NotificationStatus.SENDING,
                decision.attemptSeq(), decision.notification().resendCount())) {
            throw new IllegalStateException("완료 attempt 승자가 알림 실패 상태를 확정하지 못했습니다.");
        }
        return terminal ? FailureOutcome.TERMINAL : FailureOutcome.RETRY;
    }

    private static void requireSend(NotificationDeliveryDecision decision) {
        Objects.requireNonNull(decision, "decision");
        if (decision.action() != NotificationDeliveryDecision.Action.SEND) {
            throw new IllegalArgumentException("SEND 결정만 완료할 수 있습니다.");
        }
    }
}

package com.kafkick.storage.db.notification.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

import com.kafkick.core.notification.domain.AttemptResult;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.storage.db.support.BaseEntity;

@Entity
@Table(name = "notification_attempts")
public class NotificationAttemptEntity extends BaseEntity {
    @Column(name = "notification_id", nullable = false) private Long notificationId;
    @Column(name = "attempt_seq", nullable = false) private int attemptSeq;
    @Enumerated(EnumType.STRING) @Column(name = "`trigger`", nullable = false, length = 8)
    private AttemptTrigger trigger;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 7) private AttemptResult result;
    @Enumerated(EnumType.STRING) @Column(name = "failure_reason", length = 24)
    private NotifyFailureReason failureReason;
    @Column(name = "started_at", nullable = false) private Instant startedAt;
    @Column(name = "finished_at", nullable = false) private Instant finishedAt;

    protected NotificationAttemptEntity() { }
    public NotificationAttemptEntity(Long id, Long notificationId, int attemptSeq,
            AttemptTrigger trigger, AttemptResult result, NotifyFailureReason failureReason,
            Instant startedAt, Instant finishedAt, Instant createdAt) {
        super(id, createdAt); this.notificationId = notificationId; this.attemptSeq = attemptSeq;
        this.trigger = trigger; this.result = result; this.failureReason = failureReason;
        this.startedAt = startedAt; this.finishedAt = finishedAt;
    }
    public Long getNotificationId() { return notificationId; }
    public int getAttemptSeq() { return attemptSeq; }
    public AttemptTrigger getTrigger() { return trigger; }
    public AttemptResult getResult() { return result; }
    public NotifyFailureReason getFailureReason() { return failureReason; }
    public Instant getStartedAt() { return startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
}

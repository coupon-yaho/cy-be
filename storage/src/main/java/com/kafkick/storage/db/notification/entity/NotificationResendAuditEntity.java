package com.kafkick.storage.db.notification.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import com.kafkick.storage.db.support.BaseEntity;

@Entity
@Table(name = "notification_resend_audits")
public class NotificationResendAuditEntity extends BaseEntity {
    @Column(name = "notification_id", nullable = false) private Long notificationId;
    @Column(name = "attempt_seq") private Integer attemptSeq;
    @Column(name = "requested_by", nullable = false) private Long requestedBy;
    @Column(name = "requested_at", nullable = false) private Instant requestedAt;
    @Column(nullable = false) private boolean accepted;
    @Column(name = "reject_code", length = 12) private String rejectCode;

    protected NotificationResendAuditEntity() { }
    public NotificationResendAuditEntity(Long id, Long notificationId, Integer attemptSeq,
            Long requestedBy, Instant requestedAt, boolean accepted, String rejectCode, Instant createdAt) {
        super(id, createdAt); this.notificationId = notificationId; this.attemptSeq = attemptSeq;
        this.requestedBy = requestedBy; this.requestedAt = requestedAt;
        this.accepted = accepted; this.rejectCode = rejectCode;
    }
    public Long getNotificationId() { return notificationId; }
    public Integer getAttemptSeq() { return attemptSeq; }
    public Long getRequestedBy() { return requestedBy; }
    public Instant getRequestedAt() { return requestedAt; }
    public boolean isAccepted() { return accepted; }
    public String getRejectCode() { return rejectCode; }
}

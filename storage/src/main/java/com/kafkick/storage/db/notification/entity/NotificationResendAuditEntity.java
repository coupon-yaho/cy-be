package com.kafkick.storage.db.notification.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "notification_resend_audits")
public class NotificationResendAuditEntity {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(name = "created_at", nullable = false, updatable = false) private Instant createdAt;
    @Column(name = "notification_id", nullable = false) private Long notificationId;
    @Column(name = "attempt_seq") private Integer attemptSeq;
    @Column(name = "requested_by", nullable = false) private Long requestedBy;
    @Column(name = "requested_at", nullable = false) private Instant requestedAt;
    @Column(nullable = false) private boolean accepted;
    @Column(name = "reject_code", length = 12) private String rejectCode;

    protected NotificationResendAuditEntity() { }
    public NotificationResendAuditEntity(Long id, Long notificationId, Integer attemptSeq,
            Long requestedBy, Instant requestedAt, boolean accepted, String rejectCode, Instant createdAt) {
        this.id = id; this.createdAt = createdAt; this.notificationId = notificationId; this.attemptSeq = attemptSeq;
        this.requestedBy = requestedBy; this.requestedAt = requestedAt;
        this.accepted = accepted; this.rejectCode = rejectCode;
    }
    public Long getNotificationId() { return notificationId; }
    public Integer getAttemptSeq() { return attemptSeq; }
    public Long getRequestedBy() { return requestedBy; }
    public Instant getRequestedAt() { return requestedAt; }
    public boolean isAccepted() { return accepted; }
    public Long getId() { return id; }
    public Instant getCreatedAt() { return createdAt; }
    public String getRejectCode() { return rejectCode; }
}

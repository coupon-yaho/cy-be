package com.kafkick.storage.db.notification.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.NotificationOutboxStatus;
@Entity
@Table(name = "notification_outbox")
public class NotificationOutboxEntity {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(name = "created_at", nullable = false, updatable = false) private Instant createdAt;
    @Column(name = "notification_id", nullable = false) private Long notificationId;
    @Column(name = "attempt_seq", nullable = false) private int attemptSeq;
    @Enumerated(EnumType.STRING) @Column(name = "`trigger`", nullable = false, length = 8)
    private AttemptTrigger trigger;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 11)
    private NotificationOutboxStatus status;
    @Column(name = "failure_count", nullable = false) private int failureCount;
    @Column(name = "next_attempt_at", nullable = false, insertable = false)
    private Instant nextAttemptAt;
    @Column(name = "claimed_at") private Instant claimedAt;
    @Column(name = "claim_token", length = 36) private String claimToken;
    @Column(name = "published_at") private Instant publishedAt;

    protected NotificationOutboxEntity() { }

    public NotificationOutboxEntity(Long id, Long notificationId, int attemptSeq,
            AttemptTrigger trigger, NotificationOutboxStatus status, int failureCount,
            Instant nextAttemptAt, Instant claimedAt, String claimToken,
            Instant createdAt, Instant publishedAt) {
        this.id = id;
        this.createdAt = createdAt;
        this.notificationId = notificationId;
        this.attemptSeq = attemptSeq;
        this.trigger = trigger;
        this.status = status;
        this.failureCount = failureCount;
        this.nextAttemptAt = nextAttemptAt;
        this.claimedAt = claimedAt;
        this.claimToken = claimToken;
        this.publishedAt = publishedAt;
    }

    public Long getNotificationId() { return notificationId; }
    public int getAttemptSeq() { return attemptSeq; }
    public AttemptTrigger getTrigger() { return trigger; }
    public NotificationOutboxStatus getStatus() { return status; }
    public int getFailureCount() { return failureCount; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public Instant getClaimedAt() { return claimedAt; }
    public String getClaimToken() { return claimToken; }
    public Long getId() { return id; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
}

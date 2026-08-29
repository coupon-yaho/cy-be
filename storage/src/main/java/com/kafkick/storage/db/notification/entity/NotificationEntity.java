package com.kafkick.storage.db.notification.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.storage.db.support.UpdatableEntity;

@Entity
@Table(name = "notifications")
public class NotificationEntity extends UpdatableEntity {
    @Column(name = "coupon_id", nullable = false) private Long couponId;
    @Column(name = "member_id", nullable = false) private Long memberId;
    @Column(name = "issuance_id", nullable = false) private Long issuanceId;
    @Column(nullable = false, length = 10) private String channel;
    @Enumerated(EnumType.STRING) @Column(nullable = false, length = 8) private NotificationStatus status;
    @Column(name = "attempt_count", nullable = false) private int attemptCount;
    @Column(name = "resend_count", nullable = false) private int resendCount;
    @Enumerated(EnumType.STRING) @Column(name = "last_failure_reason", length = 24)
    private NotifyFailureReason lastFailureReason;
    @Column(name = "recipient_contact", nullable = false, length = 255)
    private String recipientContact;
    @Column(name = "message_body", nullable = false, length = 500)
    private String messageBody;
    @Column(name = "sent_at") private Instant sentAt;
    @Column(name = "failed_at") private Instant failedAt;

    protected NotificationEntity() { }

    public NotificationEntity(Long id, Long couponId, Long memberId, Long issuanceId,
            String channel, NotificationStatus status, int attemptCount, int resendCount,
            NotifyFailureReason lastFailureReason, String recipientContact, String messageBody,
            Instant createdAt, Instant updatedAt, Instant sentAt, Instant failedAt) {
        super(id, createdAt, updatedAt);
        this.couponId = couponId; this.memberId = memberId; this.issuanceId = issuanceId;
        this.channel = channel; this.status = status; this.attemptCount = attemptCount;
        this.resendCount = resendCount; this.lastFailureReason = lastFailureReason;
        this.recipientContact = recipientContact; this.messageBody = messageBody;
        this.sentAt = sentAt; this.failedAt = failedAt;
    }

    public Long getCouponId() { return couponId; }
    public Long getMemberId() { return memberId; }
    public Long getIssuanceId() { return issuanceId; }
    public String getChannel() { return channel; }
    public NotificationStatus getStatus() { return status; }
    public int getAttemptCount() { return attemptCount; }
    public int getResendCount() { return resendCount; }
    public NotifyFailureReason getLastFailureReason() { return lastFailureReason; }
    public String getRecipientContact() { return recipientContact; }
    public String getMessageBody() { return messageBody; }
    public Instant getSentAt() { return sentAt; }
    public Instant getFailedAt() { return failedAt; }
}

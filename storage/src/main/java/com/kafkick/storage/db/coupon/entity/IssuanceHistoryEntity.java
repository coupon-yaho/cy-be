// issuance_histories 테이블에 발급 상태 전이 이력을 추가 전용으로 저장합니다.
package com.kafkick.storage.db.coupon.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Table;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.support.BaseEntity;

@Entity
@Table(name = "issuance_histories")
public class IssuanceHistoryEntity extends BaseEntity {

    @Column(name = "issuance_id", nullable = false)
    private Long issuanceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 30)
    private IssuanceEventType eventType;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_status", length = 20)
    private IssuanceStatus fromStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", nullable = false, length = 20)
    private IssuanceStatus toStatus;

    @Column(length = 120)
    private String reason;

    @Column(name = "request_id", length = 36)
    private String requestId;

    protected IssuanceHistoryEntity() {
    }

    public IssuanceHistoryEntity(
            Long id,
            Long issuanceId,
            IssuanceEventType eventType,
            IssuanceStatus fromStatus,
            IssuanceStatus toStatus,
            String reason,
            String requestId,
            Instant createdAt
    ) {
        super(id, createdAt);
        this.issuanceId = issuanceId;
        this.eventType = eventType;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.reason = reason;
        this.requestId = requestId;
    }
}

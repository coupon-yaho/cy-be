package com.kafkick.storage.db.coupon.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import com.kafkick.storage.db.support.BaseEntity;

@Entity
@Table(name = "issuance_usages")
public class IssuanceUsageEntity extends BaseEntity {

    @Column(name = "issuance_id", nullable = false)
    private Long issuanceId;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "discount_amount", nullable = false)
    private int discountAmount;

    @Column(name = "used_at", nullable = false)
    private Instant usedAt;

    @Column(name = "canceled_at")
    private Instant canceledAt;

    protected IssuanceUsageEntity() {
    }

    public IssuanceUsageEntity(
            Long id,
            Long issuanceId,
            Long orderId,
            int discountAmount,
            Instant usedAt,
            Instant canceledAt
    ) {
        super(id, null);
        this.issuanceId = issuanceId;
        this.orderId = orderId;
        this.discountAmount = discountAmount;
        this.usedAt = usedAt;
        this.canceledAt = canceledAt;
    }

    public Long getIssuanceId() {
        return issuanceId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public int getDiscountAmount() {
        return discountAmount;
    }

    public Instant getUsedAt() {
        return usedAt;
    }

    public Instant getCanceledAt() {
        return canceledAt;
    }
}

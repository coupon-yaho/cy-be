// issuance_usages 테이블의 쿠폰 사용·취소 실적을 표현합니다.
package com.kafkick.storage.db.coupon.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "issuance_usages")
public class IssuanceUsageEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

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
        this.id = id;
        this.issuanceId = issuanceId;
        this.orderId = orderId;
        this.discountAmount = discountAmount;
        this.usedAt = usedAt;
        this.canceledAt = canceledAt;
    }

    public Long getId() {
        return id;
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

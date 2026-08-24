package com.kafkick.storage.db.coupon.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * 회차 ID를 자연키이자 외래키로 사용하는 재고 엔티티다.
 *
 * <p>독립된 자동 증가 ID와 created_at이 없는 테이블이므로 BaseEntity를
 * 상속하지 않는다.</p>
 */
@Entity
@Table(name = "coupon_stocks")
public class CouponStockEntity {

    @Id
    @Column(name = "coupon_id")
    private Long couponId;

    @Column(name = "total_quantity", nullable = false)
    private int totalQuantity;

    @Column(name = "active_count", nullable = false)
    private int activeCount;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected CouponStockEntity() {
    }

    public CouponStockEntity(
            Long couponId,
            int totalQuantity,
            int activeCount,
            Instant updatedAt
    ) {
        this.couponId = couponId;
        this.totalQuantity = totalQuantity;
        this.activeCount = activeCount;
        this.updatedAt = updatedAt;
    }

    public Long getCouponId() {
        return couponId;
    }

    public int getTotalQuantity() {
        return totalQuantity;
    }

    public int getActiveCount() {
        return activeCount;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}

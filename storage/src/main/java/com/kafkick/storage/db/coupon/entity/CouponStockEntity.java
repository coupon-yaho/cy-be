// coupon_stocks 테이블의 회차별 재고 상태를 표현합니다.
package com.kafkick.storage.db.coupon.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

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

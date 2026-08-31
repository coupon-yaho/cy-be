package com.kafkick.storage.db.coupon.repository;

import java.time.Instant;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.storage.db.coupon.entity.CouponStockEntity;

public interface CouponStockJpaRepository
        extends JpaRepository<CouponStockEntity, Long> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE coupon_stocks
            SET active_count = active_count + 1,
                updated_at = GREATEST(updated_at, :updatedAt, UTC_TIMESTAMP(6))
            WHERE coupon_id = :couponRoundId
              AND active_count < total_quantity
            """, nativeQuery = true)
    int occupyOne(
            @Param("couponRoundId") Long couponRoundId,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE coupon_stocks
            SET active_count = active_count + 1,
                updated_at = :updatedAt
            WHERE coupon_id = :couponRoundId
            """, nativeQuery = true)
    int incrementActiveCount(
            @Param("couponRoundId") Long couponRoundId,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE coupon_stocks
            SET active_count = active_count - :quantity,
                updated_at = GREATEST(updated_at, :updatedAt, UTC_TIMESTAMP(6))
            WHERE coupon_id = :couponRoundId
              AND active_count >= :quantity
            """, nativeQuery = true)
    int release(
            @Param("couponRoundId") Long couponRoundId,
            @Param("quantity") int quantity,
            @Param("updatedAt") Instant updatedAt
    );
}

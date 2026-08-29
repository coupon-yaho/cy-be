package com.kafkick.core.coupon.port;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponStockOccupationResult;

public interface CouponStockRepository {

    CouponStockOccupationResult occupyOne(
            Long couponRoundId,
            Instant updatedAt
    );

    /** V2가 Redis 입장 판정 뒤 같은 DB 트랜잭션에서 파생 활성 수를 올린다. */
    void incrementActiveCount(
            Long couponRoundId,
            Instant updatedAt
    );

    boolean release(
            Long couponRoundId,
            int quantity,
            Instant updatedAt
    );
}

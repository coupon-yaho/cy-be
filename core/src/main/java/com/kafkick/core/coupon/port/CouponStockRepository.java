package com.kafkick.core.coupon.port;

import java.time.Instant;

public interface CouponStockRepository {

    CouponStockOccupationResult occupy(
            Long couponRoundId,
            Instant updatedAt
    );

    boolean release(
            Long couponRoundId,
            int quantity,
            Instant updatedAt
    );
}

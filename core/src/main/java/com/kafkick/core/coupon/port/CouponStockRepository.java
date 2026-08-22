package com.kafkick.core.coupon.port;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponStockOccupationResult;

public interface CouponStockRepository {

    CouponStockOccupationResult occupyAfterLock(
            Long couponRoundId,
            Instant updatedAt
    );

    boolean lockForUpdate(Long couponRoundId);

    boolean release(
            Long couponRoundId,
            int quantity,
            Instant updatedAt
    );
}

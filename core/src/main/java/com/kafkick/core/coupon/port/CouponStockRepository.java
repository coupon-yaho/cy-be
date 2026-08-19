package com.kafkick.core.coupon.port;

import java.time.Instant;

public interface CouponStockRepository {

    void occupyOne(Long couponRoundId, Instant updatedAt);

    void lockForUpdate(Long couponRoundId);

    void releaseOneAfterLock(Long couponRoundId, Instant updatedAt);
}

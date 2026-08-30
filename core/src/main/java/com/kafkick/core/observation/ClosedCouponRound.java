package com.kafkick.core.observation;

import java.time.Instant;
import java.util.Objects;

public record ClosedCouponRound(
        long couponId,
        Instant closedAt
) {

    public ClosedCouponRound {
        if (couponId <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 ID는 양수여야 합니다."
            );
        }
        Objects.requireNonNull(closedAt, "closedAt");
    }
}

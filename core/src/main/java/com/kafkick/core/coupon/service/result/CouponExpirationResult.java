package com.kafkick.core.coupon.service.result;

public record CouponExpirationResult(
        int requestedCount,
        int expiredCount
) {
}

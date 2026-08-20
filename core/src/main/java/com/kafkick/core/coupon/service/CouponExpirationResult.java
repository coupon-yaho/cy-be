package com.kafkick.core.coupon.service;

public record CouponExpirationResult(
        int requestedCount,
        int expiredCount
) {
}

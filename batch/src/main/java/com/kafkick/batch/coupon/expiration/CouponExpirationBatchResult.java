package com.kafkick.batch.coupon.expiration;

import java.time.Instant;

public record CouponExpirationBatchResult(
        Instant asOf,
        int scannedCount,
        int expiredCount
) {
}

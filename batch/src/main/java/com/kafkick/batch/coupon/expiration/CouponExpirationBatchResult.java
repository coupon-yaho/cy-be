// 한 번의 만료 배치가 확인하고 실제 만료한 건수를 기록합니다.
package com.kafkick.batch.coupon.expiration;

import java.time.Instant;

public record CouponExpirationBatchResult(
        Instant asOf,
        int scannedCount,
        int expiredCount
) {
}

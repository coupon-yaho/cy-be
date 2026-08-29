package com.kafkick.batch.coupon.expiration;

import java.time.Instant;
import java.util.List;

public record CouponExpirationBatchResult(
        Instant asOf,
        int scannedCount,
        int expiredCount,
        /** 복원이 상한 초과({@code -2})로 거절돼 만료를 중단한 회차. 경보 대상이다. */
        List<Long> haltedRoundIds,
        /** 예외로 그 회차만 건너뛴 회차. 틱 전체를 죽이지 않는 대신 여기 남는다. */
        List<Long> failedRoundIds
) {
}

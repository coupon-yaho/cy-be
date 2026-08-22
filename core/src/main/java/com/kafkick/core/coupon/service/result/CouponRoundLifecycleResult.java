package com.kafkick.core.coupon.service.result;

public record CouponRoundLifecycleResult(
        int closedOpenCount,
        int closedMissedScheduledCount,
        int openedCount
) {

    public CouponRoundLifecycleResult {
        if (closedOpenCount < 0
                || closedMissedScheduledCount < 0
                || openedCount < 0) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 상태 전환 건수는 음수일 수 없습니다."
            );
        }
    }
}

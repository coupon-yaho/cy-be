package com.kafkick.core.coupon.service.idempotency;

import java.time.Duration;

public record IdempotencyPolicy(
        Duration waitTimeout,
        Duration pollInterval,
        Duration staleAfter
) {

    public IdempotencyPolicy {
        if (waitTimeout == null || waitTimeout.isNegative()) {
            throw new IllegalArgumentException(
                    "멱등 요청 대기 시간은 0 이상이어야 합니다."
            );
        }
        if (pollInterval == null || pollInterval.isZero()
                || pollInterval.isNegative()) {
            throw new IllegalArgumentException(
                    "멱등 요청 재조회 간격은 0보다 커야 합니다."
            );
        }
        if (staleAfter == null || staleAfter.isZero()
                || staleAfter.isNegative()) {
            throw new IllegalArgumentException(
                    "멱등 요청 회수 기준 시간은 0보다 커야 합니다."
            );
        }
    }
}

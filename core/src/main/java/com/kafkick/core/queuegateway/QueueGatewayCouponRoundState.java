package com.kafkick.core.queuegateway;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/** 외부 대기열 게이트웨이에 미러링할 한 쿠폰 회차의 재고 관측값입니다. */
public record QueueGatewayCouponRoundState(
        long couponId,
        Long remainingStock,
        SourceStatus stockStatus,
        Instant observedAt
) {

    /** 상태가 값을 싣는지에 따라 재고와 관측 시각이 함께 존재하도록 검증합니다. */
    public QueueGatewayCouponRoundState {
        Objects.requireNonNull(stockStatus, "stockStatus");
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        boolean hasValue = remainingStock != null && observedAt != null;
        if (stockStatus.carriesValue() != hasValue) {
            throw new IllegalArgumentException("stockStatus와 재고·관측 시각 조합이 일치해야 합니다.");
        }
        if ((remainingStock == null) != (observedAt == null)) {
            throw new IllegalArgumentException("remainingStock과 observedAt은 함께 존재해야 합니다.");
        }
        if (remainingStock != null && remainingStock < 0L) {
            throw new IllegalArgumentException("remainingStock은 0 이상이어야 합니다.");
        }
    }
}

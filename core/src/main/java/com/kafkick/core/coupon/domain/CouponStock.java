// 쿠폰 회차별 현재 재고 점유 수를 관리하는 도메인 모델입니다.
package com.kafkick.core.coupon.domain;

import java.time.Instant;

public record CouponStock(
        Long couponRoundId,
        int totalQuantity,
        int activeCount,
        Instant updatedAt
) {

    public CouponStock {
        if (couponRoundId != null && couponRoundId <= 0) {
            throw new IllegalArgumentException(
                    "쿠폰 회차 ID는 0보다 커야 합니다."
            );
        }
        if (totalQuantity <= 0) {
            throw new IllegalArgumentException(
                    "전체 재고는 0보다 커야 합니다."
            );
        }
        if (activeCount < 0 || activeCount > totalQuantity) {
            throw new IllegalArgumentException(
                    "현재 재고 점유 수는 0 이상 전체 재고 이하여야 합니다."
            );
        }
        if (updatedAt == null) {
            throw new IllegalArgumentException(
                    "재고 갱신 시각은 필수입니다."
            );
        }
    }

    public static CouponStock initialize(
            int totalQuantity,
            Instant updatedAt
    ) {
        return new CouponStock(null, totalQuantity, 0, updatedAt);
    }

    public static CouponStock restore(
            Long couponRoundId,
            int totalQuantity,
            int activeCount,
            Instant updatedAt
    ) {
        if (couponRoundId == null) {
            throw new IllegalArgumentException(
                    "복원할 쿠폰 회차 ID는 필수입니다."
            );
        }
        return new CouponStock(
                couponRoundId,
                totalQuantity,
                activeCount,
                updatedAt
        );
    }

    public int remainingQuantity() {
        return totalQuantity - activeCount;
    }
}

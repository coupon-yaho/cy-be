// 발급 쿠폰을 주문에 적용한 사용 실적을 표현합니다.
package com.kafkick.core.coupon.domain;

import java.time.Instant;

import com.kafkick.core.coupon.exception.CouponInvalidTransitionException;

public record IssuanceUsage(
        Long id,
        Long issuanceId,
        Long orderId,
        int discountAmount,
        Instant usedAt,
        Instant canceledAt
) {

    public IssuanceUsage {
        validateId(id, "쿠폰 사용 ID", true);
        validateId(issuanceId, "발급 ID", false);
        validateId(orderId, "주문 ID", false);
        if (discountAmount < 0) {
            throw new IllegalArgumentException(
                    "할인 금액은 0 이상이어야 합니다."
            );
        }
        if (usedAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 사용 시각은 필수입니다."
            );
        }
        if (canceledAt != null && canceledAt.isBefore(usedAt)) {
            throw new IllegalArgumentException(
                    "사용 취소 시각은 사용 시각보다 빠를 수 없습니다."
            );
        }
    }

    public static IssuanceUsage use(
            Long issuanceId,
            Long orderId,
            int discountAmount,
            Instant usedAt
    ) {
        return new IssuanceUsage(
                null,
                issuanceId,
                orderId,
                discountAmount,
                usedAt,
                null
        );
    }

    public static IssuanceUsage restore(
            Long id,
            Long issuanceId,
            Long orderId,
            int discountAmount,
            Instant usedAt,
            Instant canceledAt
    ) {
        if (id == null) {
            throw new IllegalArgumentException(
                    "복원할 쿠폰 사용 ID는 필수입니다."
            );
        }
        return new IssuanceUsage(
                id,
                issuanceId,
                orderId,
                discountAmount,
                usedAt,
                canceledAt
        );
    }

    public IssuanceUsage cancel(Instant canceledAt) {
        if (canceledAt == null) {
            throw new IllegalArgumentException(
                    "쿠폰 사용 취소 시각은 필수입니다."
            );
        }
        if (this.canceledAt != null) {
            throw new CouponInvalidTransitionException();
        }
        return new IssuanceUsage(
                id,
                issuanceId,
                orderId,
                discountAmount,
                usedAt,
                canceledAt
        );
    }

    private static void validateId(
            Long value,
            String fieldName,
            boolean nullable
    ) {
        if ((!nullable && value == null) || (value != null && value <= 0)) {
            throw new IllegalArgumentException(
                    fieldName + "는 0보다 커야 합니다."
            );
        }
    }
}

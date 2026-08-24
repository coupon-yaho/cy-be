package com.kafkick.api.coupon.dto.response;

import java.time.Instant;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.query.IssuableCouponRoundSummary;

public record IssuableCouponRoundResponse(
        Long couponRoundId,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int validDays,
        Instant openAt,
        Instant closeAt,
        int remainingQuantity
) {

    public static IssuableCouponRoundResponse from(
            IssuableCouponRoundSummary summary
    ) {
        return new IssuableCouponRoundResponse(
                summary.couponRoundId(),
                summary.brandId(),
                summary.name(),
                summary.policyType(),
                summary.discountRate(),
                summary.maxDiscountAmount(),
                summary.discountAmount(),
                summary.validDays(),
                summary.openAt(),
                summary.closeAt(),
                summary.remainingQuantity()
        );
    }
}

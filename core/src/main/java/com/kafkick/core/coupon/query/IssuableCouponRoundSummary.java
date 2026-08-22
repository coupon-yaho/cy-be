package com.kafkick.core.coupon.query;

import java.time.Instant;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;

public record IssuableCouponRoundSummary(
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
}

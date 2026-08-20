package com.kafkick.core.coupon.query;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

public record MemberCouponSummary(
        Long issuanceId,
        Long couponRoundId,
        String code,
        IssuanceStatus status,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        Instant issuedAt,
        Instant expiresAt
) {
}

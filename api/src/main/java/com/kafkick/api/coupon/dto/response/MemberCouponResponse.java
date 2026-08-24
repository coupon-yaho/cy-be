package com.kafkick.api.coupon.dto.response;

import java.time.Instant;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.query.MemberCouponSummary;

public record MemberCouponResponse(
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
        Instant expiresAt,
        Instant usedAt,
        Integer usedDiscountAmount,
        Long orderId
) {

    public static MemberCouponResponse from(MemberCouponSummary summary) {
        return new MemberCouponResponse(
                summary.issuanceId(),
                summary.couponRoundId(),
                summary.code(),
                summary.status(),
                summary.name(),
                summary.policyType(),
                summary.discountRate(),
                summary.maxDiscountAmount(),
                summary.discountAmount(),
                summary.issuedAt(),
                summary.expiresAt(),
                summary.usedAt(),
                summary.usedDiscountAmount(),
                summary.orderId()
        );
    }
}

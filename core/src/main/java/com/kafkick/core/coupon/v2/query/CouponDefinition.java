package com.kafkick.core.coupon.v2.query;

import java.time.Instant;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;

/** 재고와 회원별 발급 여부를 제외한, 조회 캐시 가능한 회차 정의다. */
public record CouponDefinition(
        long couponRoundId,
        long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int validDays,
        Instant openAt,
        Instant closeAt,
        CouponRoundStatus status
) {
}

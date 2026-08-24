package com.kafkick.core.coupon.query;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.membership.domain.MembershipGrade;

public record CouponRoundDetail(
        Long couponRoundId,
        Long templateId,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int validDays,
        Set<MembershipGrade> eligibleGrades,
        Instant openAt,
        Instant closeAt,
        CouponRoundStatus status,
        int totalQuantity,
        int remainingQuantity
) {

    public CouponRoundDetail {
        eligibleGrades = eligibleGrades.isEmpty()
                ? Collections.emptySet()
                : Collections.unmodifiableSet(EnumSet.copyOf(eligibleGrades));
    }
}

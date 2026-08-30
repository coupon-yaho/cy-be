package com.kafkick.core.coupon.query;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;

public record BrandDayCalendarEntry(
        Long templateId,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        Set<MembershipGrade> eligibleGrades,
        Instant openAt,
        Instant closeAt,
        CouponRoundStatus status,
        Long couponRoundId,
        Integer totalQuantity,
        Integer activeCount
) {

    public BrandDayCalendarEntry {
        eligibleGrades = Collections.unmodifiableSet(
                EnumSet.copyOf(eligibleGrades)
        );
    }

    public int eligibleGradesMask() {
        return MembershipGrade.toMask(eligibleGrades);
    }
}

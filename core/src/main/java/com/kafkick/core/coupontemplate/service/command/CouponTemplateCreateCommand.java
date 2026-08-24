package com.kafkick.core.coupontemplate.service.command;

import java.time.LocalTime;
import java.util.Set;

import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;

public record CouponTemplateCreateCommand(
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int validDays,
        int nthWeek,
        CouponDayOfWeek dayOfWeek,
        LocalTime startTime,
        int durationHours,
        int stockPerOccurrence,
        Set<MembershipGrade> eligibleGrades
) {
}

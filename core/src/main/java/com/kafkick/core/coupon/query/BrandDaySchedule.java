package com.kafkick.core.coupon.query;

import java.time.LocalTime;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.membership.domain.MembershipGrade;

public record BrandDaySchedule(
        Long templateId,
        Long brandId,
        String name,
        int nthWeek,
        CouponDayOfWeek dayOfWeek,
        LocalTime startTime,
        int durationHours,
        Set<MembershipGrade> eligibleGrades
) {

    public BrandDaySchedule {
        eligibleGrades = Collections.unmodifiableSet(
                EnumSet.copyOf(eligibleGrades)
        );
    }

    public int eligibleGradesMask() {
        return MembershipGrade.toMask(eligibleGrades);
    }
}

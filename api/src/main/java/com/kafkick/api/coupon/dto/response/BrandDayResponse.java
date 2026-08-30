package com.kafkick.api.coupon.dto.response;

import java.time.LocalTime;
import java.util.List;

import com.kafkick.core.coupon.query.BrandDaySchedule;
import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.membership.domain.MembershipGrade;

public record BrandDayResponse(
        Long templateId,
        Long brandId,
        String name,
        int nthWeek,
        CouponDayOfWeek dayOfWeek,
        LocalTime startTime,
        int durationHours,
        int eligibleGradesMask,
        List<MembershipGrade> eligibleGrades
) {

    public static BrandDayResponse from(BrandDaySchedule schedule) {
        return new BrandDayResponse(
                schedule.templateId(),
                schedule.brandId(),
                schedule.name(),
                schedule.nthWeek(),
                schedule.dayOfWeek(),
                schedule.startTime(),
                schedule.durationHours(),
                schedule.eligibleGradesMask(),
                schedule.eligibleGrades().stream().toList()
        );
    }
}

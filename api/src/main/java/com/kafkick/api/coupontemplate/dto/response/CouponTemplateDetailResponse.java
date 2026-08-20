package com.kafkick.api.coupontemplate.dto.response;

import java.time.LocalTime;
import java.util.List;

import com.kafkick.core.coupontemplate.domain.CouponDayOfWeek;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupontemplate.domain.CouponTemplate;
import com.kafkick.core.membership.domain.MembershipGrade;

public record CouponTemplateDetailResponse(
        Long id,
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
        List<MembershipGrade> eligibleGrades,
        boolean active
) {

    public static CouponTemplateDetailResponse from(
            CouponTemplate couponTemplate
    ) {
        return new CouponTemplateDetailResponse(
                couponTemplate.id(),
                couponTemplate.brandId(),
                couponTemplate.name(),
                couponTemplate.policyType(),
                couponTemplate.discountRate(),
                couponTemplate.maxDiscountAmount(),
                couponTemplate.discountAmount(),
                couponTemplate.validDays(),
                couponTemplate.nthWeek(),
                couponTemplate.dayOfWeek(),
                couponTemplate.startTime(),
                couponTemplate.durationHours(),
                couponTemplate.stockPerOccurrence(),
                couponTemplate.eligibleGrades().stream()
                        .sorted()
                        .toList(),
                couponTemplate.active()
        );
    }
}

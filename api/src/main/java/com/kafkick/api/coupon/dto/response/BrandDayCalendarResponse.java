package com.kafkick.api.coupon.dto.response;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.query.BrandDayCalendarEntry;
import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.membership.domain.MembershipGrade;

@JsonInclude(JsonInclude.Include.ALWAYS)
public record BrandDayCalendarResponse(
        Long templateId,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int eligibleGradesMask,
        List<MembershipGrade> eligibleGrades,
        Instant openAt,
        Instant closeAt,
        CouponRoundStatus status,
        Long couponRoundId,
        Integer totalQuantity,
        Integer activeCount,
        boolean queueActive
) {

    public static BrandDayCalendarResponse from(
            BrandDayCalendarEntry entry
    ) {
        return new BrandDayCalendarResponse(
                entry.templateId(),
                entry.brandId(),
                entry.name(),
                entry.policyType(),
                entry.discountRate(),
                entry.maxDiscountAmount(),
                entry.discountAmount(),
                entry.eligibleGradesMask(),
                entry.eligibleGrades().stream().toList(),
                entry.openAt(),
                entry.closeAt(),
                entry.status(),
                entry.couponRoundId(),
                entry.totalQuantity(),
                entry.activeCount(),
                false
        );
    }
}

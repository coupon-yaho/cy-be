// 저장이 완료된 쿠폰 템플릿 정보를 반환합니다.
package com.kafkick.api.coupon.dto;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;

import java.time.LocalTime;
import java.util.List;

public record CouponTemplateCreateResponse(
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

    public static CouponTemplateCreateResponse from(CouponTemplate couponTemplate) {
        return new CouponTemplateCreateResponse(
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

// 조회한 쿠폰 템플릿 도메인 모델을 API 응답으로 변환합니다.
package com.kafkick.api.coupon.dto;

import java.time.LocalTime;
import java.util.List;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponTemplate;
import com.kafkick.core.coupon.domain.MembershipGrade;

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

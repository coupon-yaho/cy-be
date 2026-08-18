// 쿠폰 템플릿 수정 유즈케이스에 필요한 입력값을 API 기술과 분리해 전달합니다.
package com.kafkick.core.coupon.service;

import java.time.LocalTime;
import java.util.Set;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.MembershipGrade;

public record CouponTemplateUpdateCommand(
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

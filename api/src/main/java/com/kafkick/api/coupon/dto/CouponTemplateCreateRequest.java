// 관리자 쿠폰 템플릿 생성 요청을 검증하고 코어 유즈케이스 명령으로 변환합니다.
package com.kafkick.api.coupon.dto;

import com.kafkick.core.coupon.domain.CouponDayOfWeek;
import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.MembershipGrade;
import com.kafkick.core.coupon.service.CouponTemplateCreateCommand;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.time.LocalTime;
import java.util.Set;

public record CouponTemplateCreateRequest(
        @NotNull @Positive Long brandId,
        @NotBlank @Size(max = 100) String name,
        @NotNull CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        @NotNull @Positive Integer validDays,
        @NotNull @Min(1) @Max(4) Integer nthWeek,
        @NotNull CouponDayOfWeek dayOfWeek,
        @NotNull LocalTime startTime,
        @NotNull @Positive Integer durationHours,
        @NotNull @Positive Integer stockPerOccurrence,
        @NotEmpty Set<@NotNull MembershipGrade> eligibleGrades
) {

    public CouponTemplateCreateCommand toCommand() {
        return new CouponTemplateCreateCommand(
                brandId,
                name,
                policyType,
                discountRate,
                maxDiscountAmount,
                discountAmount,
                validDays,
                nthWeek,
                dayOfWeek,
                startTime,
                durationHours,
                stockPerOccurrence,
                eligibleGrades
        );
    }
}

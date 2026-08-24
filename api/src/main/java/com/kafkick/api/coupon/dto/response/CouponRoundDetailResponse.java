package com.kafkick.api.coupon.dto.response;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupontemplate.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.membership.domain.MembershipGrade;

public record CouponRoundDetailResponse(
        Long couponRoundId,
        Long templateId,
        Long brandId,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        int validDays,
        List<MembershipGrade> eligibleGrades,
        Instant openAt,
        Instant closeAt,
        CouponRoundStatus status,
        int totalQuantity,
        int remainingQuantity
) {

    public static CouponRoundDetailResponse from(CouponRoundDetail detail) {
        return new CouponRoundDetailResponse(
                detail.couponRoundId(),
                detail.templateId(),
                detail.brandId(),
                detail.name(),
                detail.policyType(),
                detail.discountRate(),
                detail.maxDiscountAmount(),
                detail.discountAmount(),
                detail.validDays(),
                detail.eligibleGrades().stream().toList(),
                detail.openAt(),
                detail.closeAt(),
                detail.status(),
                detail.totalQuantity(),
                detail.remainingQuantity()
        );
    }
}

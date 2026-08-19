// 사용자 보유 쿠폰 한 건의 발급 상태와 할인 스냅샷을 반환합니다.
package com.kafkick.api.coupon.dto;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.port.MemberCouponSummary;

public record MemberCouponResponse(
        Long issuanceId,
        Long couponRoundId,
        String code,
        IssuanceStatus status,
        String name,
        CouponPolicyType policyType,
        Integer discountRate,
        Integer maxDiscountAmount,
        Integer discountAmount,
        Instant issuedAt,
        Instant expiresAt
) {

    public static MemberCouponResponse from(MemberCouponSummary summary) {
        return new MemberCouponResponse(
                summary.issuanceId(),
                summary.couponRoundId(),
                summary.code(),
                summary.status(),
                summary.name(),
                summary.policyType(),
                summary.discountRate(),
                summary.maxDiscountAmount(),
                summary.discountAmount(),
                summary.issuedAt(),
                summary.expiresAt()
        );
    }
}

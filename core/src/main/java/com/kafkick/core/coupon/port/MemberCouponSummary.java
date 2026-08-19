// 사용자 보유 쿠폰 목록에 필요한 발급건과 회차 스냅샷을 표현합니다.
package com.kafkick.core.coupon.port;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponPolicyType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

public record MemberCouponSummary(
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
}

// 쿠폰 발급 성공 시 발급건 식별자와 사용에 필요한 스냅샷만 반환합니다.
package com.kafkick.api.coupon.dto;

import java.time.Instant;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponIssueResponse(
        Long issuanceId,
        Long couponRoundId,
        String code,
        IssuanceStatus status,
        Instant issuedAt,
        Instant expiresAt
) {

    public static CouponIssueResponse from(Issuance issuance) {
        return new CouponIssueResponse(
                issuance.id(),
                issuance.couponRoundId(),
                issuance.code(),
                issuance.status(),
                issuance.issuedAt(),
                issuance.expiresAt()
        );
    }
}

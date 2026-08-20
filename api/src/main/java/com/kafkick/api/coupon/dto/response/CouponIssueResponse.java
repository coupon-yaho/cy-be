package com.kafkick.api.coupon.dto.response;

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

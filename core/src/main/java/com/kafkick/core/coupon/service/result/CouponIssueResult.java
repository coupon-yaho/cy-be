package com.kafkick.core.coupon.service.result;

import java.time.Instant;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponIssueResult(
        Long issuanceId,
        Long couponRoundId,
        String code,
        IssuanceStatus status,
        Instant issuedAt,
        Instant expiresAt
) {

    public static CouponIssueResult from(Issuance issuance) {
        return new CouponIssueResult(
                issuance.id(),
                issuance.couponRoundId(),
                issuance.code(),
                issuance.status(),
                issuance.issuedAt(),
                issuance.expiresAt()
        );
    }
}

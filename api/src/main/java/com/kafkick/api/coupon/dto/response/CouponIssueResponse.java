package com.kafkick.api.coupon.dto.response;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.result.CouponIssueResult;

public record CouponIssueResponse(
        Long issuanceId,
        Long couponRoundId,
        String code,
        IssuanceStatus status,
        Instant issuedAt,
        Instant expiresAt
) {

    public static CouponIssueResponse from(CouponIssueResult result) {
        return new CouponIssueResponse(
                result.issuanceId(),
                result.couponRoundId(),
                result.code(),
                result.status(),
                result.issuedAt(),
                result.expiresAt()
        );
    }
}

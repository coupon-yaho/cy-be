package com.kafkick.api.coupon.dto.response;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.result.CouponCancelResult;

public record CouponCancelResponse(
        Long issuanceId,
        IssuanceStatus status,
        Instant canceledAt
) {

    public static CouponCancelResponse from(CouponCancelResult result) {
        return new CouponCancelResponse(
                result.issuanceId(),
                result.status(),
                result.canceledAt()
        );
    }
}

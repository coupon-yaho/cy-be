package com.kafkick.api.coupon.dto.response;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;

public record CouponCancelUseResponse(
        Long issuanceId,
        IssuanceStatus status,
        Long orderId,
        int discountAmount,
        Instant canceledAt
) {

    public static CouponCancelUseResponse from(
            CouponCancelUseResult result
    ) {
        return new CouponCancelUseResponse(
                result.issuanceId(),
                result.status(),
                result.orderId(),
                result.discountAmount(),
                result.canceledAt()
        );
    }
}

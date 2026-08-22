package com.kafkick.core.coupon.service.result;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponCancelUseResult(
        Long issuanceId,
        IssuanceStatus status,
        Long orderId,
        int discountAmount,
        Instant canceledAt
) {
}

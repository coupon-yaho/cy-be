package com.kafkick.core.coupon.service.result;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponUseResult(
        Long issuanceId,
        IssuanceStatus status,
        Long orderId,
        int discountAmount,
        Instant usedAt
) {
}

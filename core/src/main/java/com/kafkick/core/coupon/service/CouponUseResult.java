package com.kafkick.core.coupon.service;

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

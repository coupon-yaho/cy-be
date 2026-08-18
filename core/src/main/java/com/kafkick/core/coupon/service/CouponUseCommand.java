package com.kafkick.core.coupon.service;

import java.time.Instant;

public record CouponUseCommand(
        Long issuanceId,
        Long memberId,
        Long orderId,
        Integer orderAmount,
        String idempotencyKey,
        Instant usedAt
) {
}

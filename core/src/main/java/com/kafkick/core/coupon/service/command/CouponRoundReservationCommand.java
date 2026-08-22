package com.kafkick.core.coupon.service.command;

import java.time.Instant;

public record CouponRoundReservationCommand(
        Long templateId,
        Instant openAt,
        Instant closeAt,
        Instant generatedAt
) {
}

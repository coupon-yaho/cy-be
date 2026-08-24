package com.kafkick.core.coupon.service.command;

import java.time.Instant;

public record CouponUseCommand(
        Long issuanceId,
        Long memberId,
        Long orderId,
        Integer orderAmount,
        String idempotencyKey,
        Instant usedAt
) {

    public static String canonicalRequest(
            Long issuanceId,
            Long memberId,
            Long orderId,
            Integer orderAmount
    ) {
        return "USE|issuanceId=" + issuanceId
                + "|memberId=" + memberId
                + "|orderId=" + orderId
                + "|orderAmount=" + orderAmount;
    }
}

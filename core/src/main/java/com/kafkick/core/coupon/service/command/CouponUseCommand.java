package com.kafkick.core.coupon.service.command;

import java.time.Instant;

public record CouponUseCommand(
        Long issuanceId,
        Long memberId,
        Integer orderAmount,
        String idempotencyKey,
        Instant usedAt
) {

    public static String canonicalRequest(
            Long issuanceId,
            Long memberId,
            Integer orderAmount
    ) {
        return "USE|issuanceId=" + issuanceId
                + "|memberId=" + memberId
                + "|orderAmount=" + orderAmount;
    }
}

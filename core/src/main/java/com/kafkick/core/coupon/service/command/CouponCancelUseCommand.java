package com.kafkick.core.coupon.service.command;

import java.time.Instant;

public record CouponCancelUseCommand(
        Long issuanceId,
        Long memberId,
        String idempotencyKey,
        Instant canceledAt
) {

    public static String canonicalRequest(Long issuanceId, Long memberId) {
        return "CANCEL_USE|issuanceId=" + issuanceId
                + "|memberId=" + memberId;
    }
}

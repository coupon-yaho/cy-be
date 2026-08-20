package com.kafkick.core.coupon.service;

import java.time.Instant;

public record CouponCancelCommand(
        Long issuanceId,
        Long memberId,
        String idempotencyKey,
        Instant canceledAt
) {

    public static String canonicalRequest(Long issuanceId, Long memberId) {
        return "CANCEL|issuanceId=" + issuanceId
                + "|memberId=" + memberId;
    }
}

package com.kafkick.core.coupon.service.result;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponCancelResult(
        Long issuanceId,
        IssuanceStatus status,
        Instant canceledAt
) {
}

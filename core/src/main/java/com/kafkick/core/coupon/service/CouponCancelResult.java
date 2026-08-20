package com.kafkick.core.coupon.service;

import java.time.Instant;

import com.kafkick.core.coupon.domain.IssuanceStatus;

public record CouponCancelResult(
        Long issuanceId,
        IssuanceStatus status,
        Instant canceledAt
) {
}

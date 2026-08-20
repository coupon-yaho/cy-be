package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupon.domain.Issuance;

public record CouponExpirationCommand(
        Long couponRoundId,
        List<Issuance> issuances,
        Instant asOf
) {
}

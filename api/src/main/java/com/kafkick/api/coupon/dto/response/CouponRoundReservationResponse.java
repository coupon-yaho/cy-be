package com.kafkick.api.coupon.dto.response;

import java.time.Instant;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponRoundStatus;

public record CouponRoundReservationResponse(
        Long id,
        Long templateId,
        Long brandId,
        String name,
        Instant openAt,
        Instant closeAt,
        CouponRoundStatus status
) {

    public static CouponRoundReservationResponse from(CouponRound round) {
        return new CouponRoundReservationResponse(
                round.id(),
                round.templateId(),
                round.brandId(),
                round.name(),
                round.openAt(),
                round.closeAt(),
                round.status()
        );
    }
}

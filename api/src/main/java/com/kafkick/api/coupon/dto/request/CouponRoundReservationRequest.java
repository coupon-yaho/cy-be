package com.kafkick.api.coupon.dto.request;

import java.time.Duration;
import java.time.Instant;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import com.kafkick.core.coupon.service.command.CouponRoundReservationCommand;

public record CouponRoundReservationRequest(
        @NotNull Instant openAt,
        @NotNull Instant closeAt
) {

    @AssertTrue(message = "쿠폰 회차 예약 시간은 24시간 이내여야 합니다.")
    public boolean isValidSchedule() {
        if (openAt == null || closeAt == null) {
            return true;
        }
        return closeAt.isAfter(openAt)
                && Duration.between(openAt, closeAt)
                .compareTo(Duration.ofHours(24)) <= 0;
    }

    public CouponRoundReservationCommand toCommand(
            Long couponTemplateId,
            Instant generatedAt
    ) {
        return new CouponRoundReservationCommand(
                couponTemplateId,
                openAt,
                closeAt,
                generatedAt
        );
    }
}

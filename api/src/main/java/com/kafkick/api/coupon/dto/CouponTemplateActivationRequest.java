package com.kafkick.api.coupon.dto;

import jakarta.validation.constraints.NotNull;

import com.kafkick.core.coupon.service.CouponTemplateActivationCommand;

public record CouponTemplateActivationRequest(
        @NotNull Boolean active
) {

    public CouponTemplateActivationCommand toCommand() {
        return new CouponTemplateActivationCommand(active);
    }
}

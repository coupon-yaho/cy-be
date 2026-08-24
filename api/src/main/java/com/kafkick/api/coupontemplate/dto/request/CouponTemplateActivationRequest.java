package com.kafkick.api.coupontemplate.dto.request;

import jakarta.validation.constraints.NotNull;

import com.kafkick.core.coupontemplate.service.command.CouponTemplateActivationCommand;

public record CouponTemplateActivationRequest(
        @NotNull Boolean active
) {

    public CouponTemplateActivationCommand toCommand() {
        return new CouponTemplateActivationCommand(active);
    }
}

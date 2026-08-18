// 관리자 쿠폰 템플릿 활성화 상태 변경 요청을 검증하고 코어 명령으로 변환합니다.
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

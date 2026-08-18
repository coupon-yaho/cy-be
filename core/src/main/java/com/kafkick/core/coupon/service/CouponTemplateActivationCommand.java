// 쿠폰 템플릿 활성화 상태 변경 유즈케이스의 입력값을 전달합니다.
package com.kafkick.core.coupon.service;

public record CouponTemplateActivationCommand(
        boolean active
) {
}

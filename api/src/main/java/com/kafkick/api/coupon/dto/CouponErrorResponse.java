// 쿠폰 API의 일관된 오류 응답 형식을 정의합니다.
package com.kafkick.api.coupon.dto;

import java.util.Map;

public record CouponErrorResponse(
        String code,
        String message,
        Map<String, String> fieldErrors
) {

    public static CouponErrorResponse of(String code, String message) {
        return new CouponErrorResponse(code, message, Map.of());
    }

    public static CouponErrorResponse of(
            String code,
            String message,
            Map<String, String> fieldErrors
    ) {
        return new CouponErrorResponse(
                code,
                message,
                Map.copyOf(fieldErrors)
        );
    }
}

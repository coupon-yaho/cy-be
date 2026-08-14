// 쿠폰 도메인에서 발생하는 비즈니스 오류 코드와 응답 정보를 정의합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponTemplateErrorCode implements ErrorCode {

    INVALID_COUPON_TEMPLATE(
            400,
            "COUPON-101",
            "쿠폰 템플릿 값이 올바르지 않습니다."
    ),

    COUPON_TEMPLATE_NOT_FOUND(
            404,
            "COUPON-102",
            "쿠폰 템플릿을 찾을 수 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    CouponTemplateErrorCode(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}

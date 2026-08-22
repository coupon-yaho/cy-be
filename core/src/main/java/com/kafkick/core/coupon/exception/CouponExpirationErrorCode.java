package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponExpirationErrorCode implements ErrorCode {

    COUPON_EXPIRATION_SAVE_FAILED(
            500,
            "COUPON-414",
            "쿠폰 만료 저장 중 오류가 발생했습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    CouponExpirationErrorCode(int status, String code, String message) {
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

package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponQueryErrorCode implements ErrorCode {

    MEMBER_COUPON_NOT_FOUND(
            404,
            "COUPON-415",
            "보유 쿠폰을 찾을 수 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    CouponQueryErrorCode(int status, String code, String message) {
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

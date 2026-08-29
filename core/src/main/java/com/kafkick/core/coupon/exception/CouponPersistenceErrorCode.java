package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponPersistenceErrorCode implements ErrorCode {

    COUPON_PERSISTENCE_FAILED(
            500,
            "COUPON-500",
            "쿠폰 데이터 저장 중 오류가 발생했습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    CouponPersistenceErrorCode(int status, String code, String message) {
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

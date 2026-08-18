// 쿠폰 회차 생성 과정의 도메인별 오류 코드와 응답 정보를 정의합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponRoundErrorCode implements ErrorCode {

    COUPON_ROUND_ALREADY_EXISTS(
            409,
            "COUPON-201",
            "동일한 일정의 쿠폰 회차가 이미 존재합니다."
    ),

    COUPON_ROUND_SAVE_FAILED(
            500,
            "COUPON-202",
            "쿠폰 회차 저장 중 오류가 발생했습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    CouponRoundErrorCode(int status, String code, String message) {
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

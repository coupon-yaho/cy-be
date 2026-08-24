package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponRoundErrorCode implements ErrorCode {

    COUPON_ROUND_ALREADY_EXISTS(
            409,
            "COUPON_ROUND-201",
            "동일한 일정의 쿠폰 회차가 이미 존재합니다."
    ),

    COUPON_ROUND_SCHEDULE_CONFLICT(
            409,
            "COUPON_ROUND-202",
            "해당 시간에는 다른 쿠폰 발급 이벤트가 예약되어 있습니다."
    ),

    INVALID_COUPON_ROUND_SCHEDULE(
            400,
            "COUPON_ROUND-203",
            "쿠폰 회차 예약 시간이 올바르지 않습니다."
    ),

    COUPON_ROUND_NOT_FOUND(
            404,
            "COUPON_ROUND-204",
            "쿠폰 회차를 찾을 수 없습니다."
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

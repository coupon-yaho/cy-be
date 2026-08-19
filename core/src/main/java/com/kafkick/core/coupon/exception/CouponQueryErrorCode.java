// 사용자 쿠폰 조회 과정의 저장 실패를 표준 응답 코드로 정의합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponQueryErrorCode implements ErrorCode {

    COUPON_QUERY_FAILED(
            500,
            "COUPON-415",
            "쿠폰 조회 중 오류가 발생했습니다."
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

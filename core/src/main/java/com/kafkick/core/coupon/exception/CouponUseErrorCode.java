// 쿠폰 사용 과정의 비즈니스 실패를 공통 응답 코드로 정의합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.ErrorCode;

public enum CouponUseErrorCode implements ErrorCode {

    INVALID_COUPON_USE_REQUEST(400, "COUPON-400", "쿠폰 사용 요청 값이 올바르지 않습니다."),
    ISSUANCE_NOT_FOUND(404, "COUPON-401", "발급된 쿠폰을 찾을 수 없습니다."),
    NOT_COUPON_OWNER(403, "COUPON-402", "본인 소유의 쿠폰만 사용할 수 있습니다."),
    COUPON_EXPIRED(409, "COUPON-403", "만료된 쿠폰은 사용할 수 없습니다."),
    IDEMPOTENCY_KEY_REUSED(422, "COUPON-404", "멱등키가 다른 요청에 이미 사용되었습니다."),
    CONFLICT_IN_PROGRESS(409, "COUPON-405", "동일한 요청을 처리하고 있습니다."),
    COUPON_USE_SAVE_FAILED(500, "COUPON-406", "쿠폰 사용 저장 중 오류가 발생했습니다."),
    IDEMPOTENCY_SAVE_FAILED(500, "COUPON-407", "멱등 처리 결과 저장 중 오류가 발생했습니다.");

    private final int status;
    private final String code;
    private final String message;

    CouponUseErrorCode(int status, String code, String message) {
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

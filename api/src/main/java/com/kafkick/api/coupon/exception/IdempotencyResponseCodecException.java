package com.kafkick.api.coupon.exception;

import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.support.exception.ErrorCode;

public class IdempotencyResponseCodecException extends RuntimeException {

    private final transient ErrorCode errorCode;

    public IdempotencyResponseCodecException(
            String detail,
            Throwable cause
    ) {
        super(detail, cause);
        this.errorCode = CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}

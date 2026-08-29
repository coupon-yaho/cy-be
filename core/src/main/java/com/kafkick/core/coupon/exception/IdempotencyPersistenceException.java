package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class IdempotencyPersistenceException extends BusinessException {

    public IdempotencyPersistenceException(String detail, Throwable cause) {
        super(CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED, detail, cause);
    }

    public IdempotencyPersistenceException(String detail) {
        super(CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED, detail);
    }
}

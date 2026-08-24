package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponPersistenceException extends BusinessException {

    public CouponPersistenceException(String detail, Throwable cause) {
        super(
                CouponPersistenceErrorCode.COUPON_PERSISTENCE_FAILED,
                detail,
                cause
        );
    }
}

package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponExpirationPersistenceException
        extends BusinessException {

    public CouponExpirationPersistenceException(
            String detail,
            Throwable cause
    ) {
        super(
                CouponExpirationErrorCode.COUPON_EXPIRATION_SAVE_FAILED,
                detail,
                cause
        );
    }
}

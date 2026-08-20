package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponUsePersistenceException extends BusinessException {

    public CouponUsePersistenceException(String detail, Throwable cause) {
        super(CouponUseErrorCode.COUPON_USE_SAVE_FAILED, detail, cause);
    }
}

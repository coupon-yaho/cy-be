package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponCancelPersistenceException extends BusinessException {

    public CouponCancelPersistenceException(
            String detail,
            Throwable cause
    ) {
        super(CouponUseErrorCode.COUPON_CANCEL_SAVE_FAILED, detail, cause);
    }
}

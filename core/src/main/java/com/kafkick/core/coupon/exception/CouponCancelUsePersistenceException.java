package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponCancelUsePersistenceException extends BusinessException {

    public CouponCancelUsePersistenceException(
            String detail,
            Throwable cause
    ) {
        super(
                CouponUseErrorCode.COUPON_CANCEL_USE_SAVE_FAILED,
                detail,
                cause
        );
    }
}

package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponStockLockPersistenceException extends BusinessException {

    public CouponStockLockPersistenceException(
            String detail,
            Throwable cause
    ) {
        super(
                CouponUseErrorCode.COUPON_STOCK_LOCK_FAILED,
                detail,
                cause
        );
    }
}

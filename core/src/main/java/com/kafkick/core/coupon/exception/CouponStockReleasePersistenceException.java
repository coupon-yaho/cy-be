package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponStockReleasePersistenceException
        extends BusinessException {

    public CouponStockReleasePersistenceException(
            String detail,
            Throwable cause
    ) {
        super(
                CouponUseErrorCode.COUPON_STOCK_RELEASE_FAILED,
                detail,
                cause
        );
    }
}

package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponQueryPersistenceException extends BusinessException {

    public CouponQueryPersistenceException(String detail, Throwable cause) {
        super(CouponQueryErrorCode.COUPON_QUERY_FAILED, detail, cause);
    }
}

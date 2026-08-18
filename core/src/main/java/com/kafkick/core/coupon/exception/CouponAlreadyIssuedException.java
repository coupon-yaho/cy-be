package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponAlreadyIssuedException extends BusinessException {

    public CouponAlreadyIssuedException(String detail, Throwable cause) {
        super(CouponIssueErrorCode.ALREADY_ISSUED, detail, cause);
    }
}

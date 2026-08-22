package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponInvalidTransitionException extends BusinessException {

    public CouponInvalidTransitionException() {
        super(CouponIssueErrorCode.INVALID_TRANSITION);
    }
}

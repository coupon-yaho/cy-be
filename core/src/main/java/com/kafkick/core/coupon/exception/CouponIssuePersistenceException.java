package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponIssuePersistenceException extends BusinessException {

    public CouponIssuePersistenceException(String detail, Throwable cause) {
        super(CouponIssueErrorCode.COUPON_ISSUE_SAVE_FAILED, detail, cause);
    }
}

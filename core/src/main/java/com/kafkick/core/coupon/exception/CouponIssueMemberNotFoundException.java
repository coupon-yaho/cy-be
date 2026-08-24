package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponIssueMemberNotFoundException extends BusinessException {

    public CouponIssueMemberNotFoundException(
            String detail,
            Throwable cause
    ) {
        super(CouponIssueErrorCode.MEMBER_NOT_FOUND, detail, cause);
    }
}

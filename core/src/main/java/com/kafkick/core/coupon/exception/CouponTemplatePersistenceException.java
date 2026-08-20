package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponTemplatePersistenceException extends BusinessException {

    public CouponTemplatePersistenceException(
            String detail,
            Throwable cause
    ) {
        super(
                CouponTemplateErrorCode.INVALID_COUPON_TEMPLATE,
                detail,
                cause
        );
    }
}

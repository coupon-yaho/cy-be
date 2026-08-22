package com.kafkick.core.coupontemplate.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponTemplatePersistenceException extends BusinessException {

    public CouponTemplatePersistenceException(
            String detail,
            Throwable cause
    ) {
        super(
                CouponTemplateErrorCode.COUPON_TEMPLATE_SAVE_FAILED,
                detail,
                cause
        );
    }
}

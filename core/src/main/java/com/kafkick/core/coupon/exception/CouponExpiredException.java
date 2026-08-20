package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponExpiredException extends BusinessException {

    public CouponExpiredException(Long issuanceId) {
        super(
                CouponUseErrorCode.COUPON_EXPIRED,
                "issuanceId=" + issuanceId
        );
    }
}

package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponRoundAlreadyExistsException extends BusinessException {

    public CouponRoundAlreadyExistsException(String message) {
        super(CouponRoundErrorCode.COUPON_ROUND_ALREADY_EXISTS, message);
    }

    public CouponRoundAlreadyExistsException(String message, Throwable cause) {
        super(
                CouponRoundErrorCode.COUPON_ROUND_ALREADY_EXISTS,
                message,
                cause
        );
    }
}

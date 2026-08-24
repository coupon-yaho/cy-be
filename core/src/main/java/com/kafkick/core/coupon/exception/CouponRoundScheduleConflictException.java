package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponRoundScheduleConflictException extends BusinessException {

    public CouponRoundScheduleConflictException(String detail) {
        super(CouponRoundErrorCode.COUPON_ROUND_SCHEDULE_CONFLICT, detail);
    }
}

// 동일 템플릿과 오픈 시각의 회차가 이미 존재함을 나타냅니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponRoundAlreadyExistsException extends BusinessException {

    public CouponRoundAlreadyExistsException(String message, Throwable cause) {
        super(
                CouponRoundErrorCode.COUPON_ROUND_ALREADY_EXISTS,
                message,
                cause
        );
    }
}

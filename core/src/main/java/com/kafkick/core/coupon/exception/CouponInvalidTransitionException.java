// 허용되지 않은 쿠폰 상태 전이를 409 비즈니스 오류로 변환합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponInvalidTransitionException extends BusinessException {

    public CouponInvalidTransitionException() {
        super(CouponIssueErrorCode.INVALID_TRANSITION);
    }
}

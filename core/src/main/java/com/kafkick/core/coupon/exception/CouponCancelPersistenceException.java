// 쿠폰 발급 취소 영속화 실패를 표준 쿠폰 오류 코드로 변환합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponCancelPersistenceException extends BusinessException {

    public CouponCancelPersistenceException(
            String detail,
            Throwable cause
    ) {
        super(CouponUseErrorCode.COUPON_CANCEL_SAVE_FAILED, detail, cause);
    }
}

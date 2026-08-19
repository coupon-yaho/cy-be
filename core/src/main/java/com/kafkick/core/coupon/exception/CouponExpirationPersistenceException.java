// 쿠폰 만료 영속화 실패를 표준 쿠폰 오류 코드로 변환합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponExpirationPersistenceException
        extends BusinessException {

    public CouponExpirationPersistenceException(
            String detail,
            Throwable cause
    ) {
        super(
                CouponExpirationErrorCode.COUPON_EXPIRATION_SAVE_FAILED,
                detail,
                cause
        );
    }
}

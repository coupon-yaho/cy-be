// 만료 시각이 지난 쿠폰 사용을 409 비즈니스 오류로 변환합니다.
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

// 쿠폰 사용 실적 저장 실패를 공통 500 오류로 변환합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponUsePersistenceException extends BusinessException {

    public CouponUsePersistenceException(String detail, Throwable cause) {
        super(CouponUseErrorCode.COUPON_USE_SAVE_FAILED, detail, cause);
    }
}

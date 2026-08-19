// 사용자 쿠폰 조회 저장소 실패를 표준 쿠폰 오류로 변환합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class CouponQueryPersistenceException extends BusinessException {

    public CouponQueryPersistenceException(String detail, Throwable cause) {
        super(CouponQueryErrorCode.COUPON_QUERY_FAILED, detail, cause);
    }
}

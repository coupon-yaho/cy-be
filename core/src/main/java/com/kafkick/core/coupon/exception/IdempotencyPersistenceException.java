// 멱등 레코드 영속화 실패를 공통 500 오류로 변환합니다.
package com.kafkick.core.coupon.exception;

import com.kafkick.core.support.exception.BusinessException;

public class IdempotencyPersistenceException extends BusinessException {

    public IdempotencyPersistenceException(String detail, Throwable cause) {
        super(CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED, detail, cause);
    }

    public IdempotencyPersistenceException(String detail) {
        super(CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED, detail);
    }
}

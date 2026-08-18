// 동일 템플릿과 오픈 시각의 회차가 이미 존재함을 나타냅니다.
package com.kafkick.core.coupon.exception;

public class CouponRoundAlreadyExistsException extends RuntimeException {

    public CouponRoundAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}

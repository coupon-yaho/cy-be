// 쿠폰 회차와 최초 재고를 저장하지 못한 내부 영속성 오류를 나타냅니다.
package com.kafkick.core.coupon.exception;

public class CouponRoundPersistenceException extends RuntimeException {

    public CouponRoundPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}

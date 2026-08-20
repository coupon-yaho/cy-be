package com.kafkick.api.coupon.exception;

public class IdempotencyResponseCodecException extends RuntimeException {

    public IdempotencyResponseCodecException(
            String detail,
            Throwable cause
    ) {
        super(detail, cause);
    }
}

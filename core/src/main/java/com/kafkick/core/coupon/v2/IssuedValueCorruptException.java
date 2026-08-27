package com.kafkick.core.coupon.v2;

public final class IssuedValueCorruptException extends RuntimeException {

    public IssuedValueCorruptException(String message) {
        super(message);
    }

    public IssuedValueCorruptException(String message, Throwable cause) {
        super(message, cause);
    }
}

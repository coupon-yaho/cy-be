package com.kafkick.core.coupon.port;

public interface IdempotencyResultCodec<R> {

    String write(R result);

    R read(String responseBody);
}

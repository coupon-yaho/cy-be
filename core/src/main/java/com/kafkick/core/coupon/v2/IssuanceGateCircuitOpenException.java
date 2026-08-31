package com.kafkick.core.coupon.v2;

/** 선점 Lua를 보내지 않은 차단기 개방. 보상할 선점도 없다. */
public final class IssuanceGateCircuitOpenException extends RuntimeException {

    public IssuanceGateCircuitOpenException(Throwable cause) {
        super("Redis 발급 차단기가 열려 있습니다.", cause);
    }
}

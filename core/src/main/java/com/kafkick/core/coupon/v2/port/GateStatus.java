package com.kafkick.core.coupon.v2.port;

/**
 * 게이트 상태. <b>스크립트의 판정과 같은 이분법</b>이다 — Lua 는 {@code meta.status ~= 'OPEN'}
 * 하나로만 보므로, 여기서 값을 더 늘리면 문서에 없는 상태가 전부 조용히 "마감" 으로 접힌다.
 */
public enum GateStatus {

    OPEN,
    CLOSED;

    /** Redis 에 적히는 문자열. 이 이름이 곧 계약이라 {@code name()} 을 그대로 쓴다. */
    public String wireValue() {
        return name();
    }

    /**
     * @param wireValue Redis 에 적혀 있던 값
     * @return {@code 'OPEN'} 이면 {@link #OPEN}, 나머지는 전부 {@link #CLOSED}
     */
    public static GateStatus fromWireValue(String wireValue) {
        return OPEN.wireValue().equals(wireValue) ? OPEN : CLOSED;
    }
}

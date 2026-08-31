package com.kafkick.core.coupon.v2.port;

/** 보상 CAS 결과. */
public enum CompensateOutcome {

    REVERTED,
    /** 이미 없거나 내 선점이 아니다 — 아무것도 하지 않았다. 정상이다. */
    NOT_MINE,
    /** 이미 {@code D} 다. 보상 금지 — 경보. */
    ALREADY_DONE,
    CORRUPT_VALUE,
    /** 되돌리기 <b>전에</b> 본 카운터가 읽히지 않았다. 아무것도 적용되지 않았다. */
    COUNTER_UNREADABLE,
    BAD_ARGUMENT
}

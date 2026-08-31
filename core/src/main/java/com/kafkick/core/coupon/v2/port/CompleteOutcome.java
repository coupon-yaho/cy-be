package com.kafkick.core.coupon.v2.port;

/** 완료 CAS 결과. */
public enum CompleteOutcome {

    PROMOTED,
    /** 이미 {@code D} 다 — 재시도끼리 겹친 것이라 정상이다. */
    ALREADY_DONE,
    /** field 가 사라졌다. 보상과 겹쳤다. */
    CLAIM_GONE,
    /** 남의 선점이다. */
    FOREIGN_CLAIM,
    CORRUPT_VALUE,
    BAD_ARGUMENT
}

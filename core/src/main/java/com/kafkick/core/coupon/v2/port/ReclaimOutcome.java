package com.kafkick.core.coupon.v2.port;

/** 파손 값 회수 결과. 호출 지점은 재구성 절차 하나뿐이다(13 문서). */
public enum ReclaimOutcome {

    /** 지우고 {@code stock}·{@code issued_ever} 까지 되돌렸다 — DB 에 발급이 없었다. */
    RECLAIMED_AND_RESTORED,
    /** field 만 지웠다 — DB 에 발급이 있어 재고를 되살리면 초과 발급이다. */
    RECLAIMED_ONLY,
    /** field 가 이미 없다. 정상이다. */
    NOTHING,
    /** 파손이 아니다 — 살아 있는 선점이라 건드리지 않았다. */
    NOT_CORRUPT,
    COUNTER_UNREADABLE,
    /** 상한 초과. 파손 값에는 재고를 깎았다는 증거가 없다. */
    OVER_CAP,
    BAD_ARGUMENT
}

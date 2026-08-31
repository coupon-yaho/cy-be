package com.kafkick.core.coupon.v2.port;

/** 만료 배치의 재고 복원 결과. */
public enum RestoreOutcome {

    RESTORED,
    /** 게이트 미준비(재구성 창). 건너뛰고 건수를 남긴다. */
    GATE_NOT_READY,
    /** 상한 초과. 그 회차 만료 처리를 멈추고 경보한다. */
    OVER_CAP,
    BAD_ARGUMENT,
    /**
     * {@code stock} 을 읽을 수 없다. 이름이 선점의 {@code COUNTER_UNREADABLE} 과 다른 이유는
     * <b>이름이 점검 범위</b>이기 때문이다 — 복원은 {@code stock} 하나만 본다.
     */
    STOCK_MISSING
}

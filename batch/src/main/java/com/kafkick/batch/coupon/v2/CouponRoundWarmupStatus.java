package com.kafkick.batch.coupon.v2;

/**
 * 워밍업 한 번의 결말. <b>거절을 예외로 만들지 않는다</b> — 부하 스크립트가 회차마다 이걸
 * 그대로 읽고 다음 단계를 정하므로, 스택트레이스가 아니라 이름이 나가야 한다.
 */
public enum CouponRoundWarmupStatus {

    /** 세 키와 {@code active_count} 를 쓰고 {@code meta} 로 게이트를 열었다. */
    WARMED,

    /**
     * {@code meta} 가 이미 있다. <b>아무것도 쓰지 않고 돌아간다.</b> 이미 열린 게이트 뒤에서
     * 카운터를 통째로 갈아엎는 것이 07 이 초과 발급 경로라고 적은 그림 그대로다.
     * 다시 만드는 경로(게이트를 닫고 재구성)는 S8b 다.
     */
    GATE_ALREADY_OPEN,

    /**
     * 같은 회차의 워밍업이 이 인스턴스에서 이미 돌고 있다.
     *
     * <p><b>batch 가 1대라는 사실은 이것을 막지 못한다.</b> 트리거가 HTTP 라 워커 스레드가
     * 여럿이고, 07 이 그린 겹침 시퀀스는 프로세스 수가 아니라 <b>실행의 겹침</b>으로 성립한다 —
     * 늦게 시작한 쪽이 먼저 열린 게이트 뒤에서 {@code issued} 를 지우고 {@code stock} 을
     * 되돌린다.
     */
    WARMUP_IN_PROGRESS,

    /** 회차 행이 없다. */
    ROUND_NOT_FOUND,

    /** {@code coupon_stocks} 행이 없다. 총재고를 모르면 {@code stock} 도 {@code meta} 도 못 쓴다. */
    STOCK_ROW_MISSING,

    /** 회차 엔진이 V2 가 아니다. v1 회차에 v2 키를 올리면 아무도 안 읽는 카운터가 생긴다. */
    ENGINE_NOT_V2,

    /**
     * 이미 오픈 시각을 지났다. 워밍업은 <b>아직 열리지 않은 회차</b>를 올리는 것이고,
     * 살아있는 회차를 v1 → v2 로 전환하는 경로는 만들지 않는다(10 의 "범위에서 빠진 것").
     */
    ROUND_ALREADY_OPENED,

    /**
     * 활성 건수가 총재고를 넘는다. {@code stock} 이 음수로 열린다는 뜻이고, 그 전에
     * <b>DB 에 이미 §9.1 I1(초과 발급) 위반이 있다</b>는 뜻이다. 헤드라인이 "초과 발급 0건" 인
     * 프로젝트에서 그 조건을 처음 만나는 코드가 여기라, 성공으로 보고하지 않는다.
     */
    OVER_ISSUED_ROUND
}

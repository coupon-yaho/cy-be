package com.kafkick.core.observation;

public enum ReasonCode {

    NOT_OPENED,
    CAMPAIGN_CLOSED,
    GRADE_NOT_ELIGIBLE,
    QUEUE_REQUIRED,
    NO_ENTRY_TOKEN,
    ENTRY_TOKEN_EXPIRED,
    ALREADY_ISSUED,
    STOCK_EXHAUSTED,
    INVALID_TRANSITION,
    TEMPORARILY_UNAVAILABLE,
    INTERNAL_ERROR,
    /**
     * v2 {@code -7}. 같은 멱등키가 아직 처리 중이다. 폴링하지 않고 409 로 떨어뜨린다.
     *
     * <p><b>{@code -6}(완료된 재시도)에 대응하는 값은 여기 없다.</b> 그건 최초 응답을 그대로
     * 돌려주는 201 성공이고, 성공 이벤트에는 이유 코드를 실을 수 없다 —
     * {@code issue_attempts} 의 CHECK 제약도 같은 규칙이다. {@code -6} 은 attempt 의
     * {@code replayed} 플래그와 {@code app.issuance.v2.replay.done} 카운터가 가른다.
     */
    REPLAY_IN_PROGRESS,
    /** v2 {@code -8}. 값 파손 — 자리가 잠기고 재고 한 장이 증발한 신호다. */
    VALUE_CORRUPT,
    /** v2 {@code -9}. 게이트 미준비. 재구성 창 안에서는 정상이고 밖에서는 이상이다. */
    GATE_NOT_READY,
    /** v2 {@code -10}. 인자 이상 — 전적으로 호출부 버그다. */
    BAD_ARGUMENT,
    /** v2 {@code -11}. 카운터를 못 읽는다. 기다려서 풀리지 않아 {@link #GATE_NOT_READY} 와 다르다. */
    COUNTER_UNREADABLE,
    UNMAPPED
}

package com.kafkick.core.coupon.v2.port;

/**
 * 선점 결과. 숫자 반환 코드는 어댑터 안에서 끝난다 — 같은 {@code -2} 가 스크립트마다 다른
 * 뜻이라 호출부까지 흘리면 응답 매핑과 경보가 조용히 어긋난다.
 */
public enum ClaimOutcome {

    CLAIMED,
    CLOSED,
    NOT_OPEN,
    GRADE_NOT_ALLOWED,
    /** 이 회원이 <b>다른 멱등키로</b> 이미 받았다. 재시도와 합치지 않는다. */
    DUP_PER_MEMBER,
    SOLD_OUT,
    /** 같은 멱등키의 재시도이고 이미 완료됐다 — 최초 응답을 재사용한다. */
    REPLAY_DONE,
    /** 같은 멱등키의 재시도이고 아직 처리 중이다. */
    REPLAY_PENDING,
    /** 값 형식 파손. 정상 운영에서 0 이어야 한다. 회수는 별도 경로다(13 문서). */
    CORRUPT_VALUE,
    /** 게이트 미준비 — 재구성 창이다. <b>기다리면 풀린다.</b> */
    GATE_NOT_READY,
    /** 인자 이상. 호출부 버그다. */
    BAD_ARGUMENT,
    /**
     * {@code stock}·{@code issued_ever} 를 읽을 수 없다. <b>매진이 아니다</b> —
     * 합쳐 두면 재고가 남았는데도 전량 종단 거절된다. {@link #GATE_NOT_READY} 와도 다르다:
     * 이건 사람이 봐야 풀린다.
     */
    COUNTER_UNREADABLE;

    public boolean isClaimed() {
        return this == CLAIMED;
    }
}

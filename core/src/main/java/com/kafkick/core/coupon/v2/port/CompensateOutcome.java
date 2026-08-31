package com.kafkick.core.coupon.v2.port;

/** 보상 CAS 결과. */
public enum CompensateOutcome {

    REVERTED,
    /** 차단기가 열려 Redis에 보상 명령을 보내지 않았다. 선점이 남았을 가능성이 있다. */
    NOT_ATTEMPTED_CIRCUIT_OPEN,
    /**
     * 보상 명령을 보냈으나 호출 자체가 실패했다 — <b>가장 불확실한 상태다.</b>
     * 스크립트가 돌았는지조차 모르므로 {@code null}(보상을 시도하지 않기로 한 결정)과
     * 구분한다. 그 둘을 뭉치면 "보상이 깨졌다"가 "보상할 것이 없었다"와 같은 칸에 들어가고,
     * 제일 위험한 경로만 재시도 안내 없는 500 으로 새어 나간다.
     */
    ATTEMPT_FAILED,
    /**
     * 되돌릴 선점이 없다 — 다른 절차가 이미 정리했다. 이 요청이 게이트에 남긴 것이 없다.
     */
    NO_CLAIM,
    /**
     * 그 자리를 다른 토큰이 잡고 있다 — 내 선점이 덮였고 {@code DECR} 은 복구되지 않았다.
     * <b>{@link #NO_CLAIM} 과 반대다.</b> 예전에는 둘이 Lua 의 {@code 0} 하나로 접혀 있어
     * 이 값을 읽는 자리마다 둘 중 하나를 추측해야 했고, 그래서 같은 값이 한쪽에서는
     * "정상", 다른 쪽에서는 "누수"로 판정되는 일이 반복됐다.
     */
    NOT_MINE,
    /** 이미 {@code D} 다. 보상 금지 — 경보. */
    ALREADY_DONE,
    CORRUPT_VALUE,
    /** 되돌리기 <b>전에</b> 본 카운터가 읽히지 않았다. 아무것도 적용되지 않았다. */
    COUNTER_UNREADABLE,
    BAD_ARGUMENT
}

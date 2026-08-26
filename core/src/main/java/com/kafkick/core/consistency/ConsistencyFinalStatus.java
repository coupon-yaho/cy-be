package com.kafkick.core.consistency;

/** 회차 수명주기와 독립인 FINAL 정합성 계산·저장 상태입니다. */
public enum ConsistencyFinalStatus {
    NONE,
    IN_PROGRESS,
    DONE,
    FAILED,
    /** 회차 확정으로부터 허용 지연을 넘겨 더 이상 그 회차의 값을 얻을 수 없는 종결 상태입니다. */
    EXPIRED
}

package com.kafkick.core.consistency;

/** 회차 수명주기와 독립인 FINAL 정합성 계산·저장 상태입니다. */
public enum ConsistencyFinalStatus {
    NONE,
    IN_PROGRESS,
    DONE,
    FAILED
}

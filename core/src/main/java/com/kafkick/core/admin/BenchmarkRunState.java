package com.kafkick.core.admin;

/** 벤치마크 실행과 FINAL 판정의 수명주기 상태입니다. */
public enum BenchmarkRunState {
    CREATED,
    RUNNING,
    STOPPING,
    STOPPED,
    FINALIZING,
    FINALIZED,
    FAILED
}

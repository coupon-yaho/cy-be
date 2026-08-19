package com.kafkick.core.consistency;

/** 정합성 평가가 실행되는 운영 단계를 구분합니다. */
public enum ConsistencyPhase {

    /** 부하 실행 중 관측값의 경보 수준을 계산하는 단계입니다. */
    LIVE,

    /** 부하 종료와 안정화 이후 엄격한 PASS/FAIL을 확정하는 단계입니다. */
    FINAL
}

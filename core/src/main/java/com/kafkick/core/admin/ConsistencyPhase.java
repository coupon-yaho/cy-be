package com.kafkick.core.admin;

/** 부하 중 경량 관측과 종료 후 권위 집계를 구분합니다. */
public enum ConsistencyPhase {
    LIVE,
    FINAL
}

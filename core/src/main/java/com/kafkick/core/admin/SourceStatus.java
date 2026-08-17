package com.kafkick.core.admin;

/** 관측값이 숫자로 해석될 수 있는지와 원천의 현재 상태를 구분합니다. */
public enum SourceStatus {
    VALID,
    PENDING,
    WARMING_UP,
    STALE,
    NO_TRAFFIC,
    UNAVAILABLE,
    N_A
}

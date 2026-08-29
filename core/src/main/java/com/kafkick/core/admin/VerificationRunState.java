package com.kafkick.core.admin;

/** 관리자 전수 검증 실행의 수명주기 상태입니다. */
public enum VerificationRunState {
    REQUESTED,
    RUNNING,
    COMPLETED,
    FAILED
}

package com.kafkick.core.admin;

/** 대기열의 실제 운영 상태이며 설정값인 {@link QueueMode}와 구분됩니다. */
public enum QueueState {
    IDLE,
    QUEUEING,
    DRAINING
}

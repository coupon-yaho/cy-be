package com.kafkick.core.support.exception;

import com.kafkick.core.observation.ReasonCode;

import java.util.Optional;

public interface ErrorCode {

    int getStatus();

    String getCode();

    String getMessage();

    /** 기존 구현은 매핑을 생략할 수 있으며, A-04가 빈 값을 UNMAPPED로 기록한다. */
    default Optional<ReasonCode> reasonCode() {
        return Optional.empty();
    }
}

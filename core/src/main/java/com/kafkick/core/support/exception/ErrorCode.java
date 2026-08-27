package com.kafkick.core.support.exception;

import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.Dependency;

import java.util.Optional;

public interface ErrorCode {

    int getStatus();

    String getCode();

    String getMessage();

    /** 기존 구현은 매핑을 생략할 수 있으며, A-04가 빈 값을 UNMAPPED로 기록한다. */
    default Optional<ReasonCode> reasonCode() {
        return Optional.empty();
    }

    /**
     * 오류를 직접 일으킨 외부 의존성. 애플리케이션·클라이언트 오류는 {@link Dependency#NONE}.
     *
     * <p>현재 저장소에는 인프라 실패 ErrorCode producer가 없다. OBS-5에서 Redis·MySQL·Kafka
     * 어댑터 오류를 추가할 때 반드시 실제 의존성을 override해야 HTTP dependency failure가
     * 도달 가능해진다.
     */
    default Dependency dependency() {
        return Dependency.NONE;
    }
}

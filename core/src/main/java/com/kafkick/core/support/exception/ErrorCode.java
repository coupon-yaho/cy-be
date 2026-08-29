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

    /**
     * 이 코드의 5xx 를 스택트레이스와 함께 남길지.
     *
     * <p>기본이 {@code true} 인 이유는 5xx 가 보통 예상 밖 실패라서다. 다만 <b>정상 흐름에서
     * 대량 발생하는 5xx</b> — 의존성 장애 동안의 조회 완화 응답 같은 것 — 은 예외다. 고QPS
     * 경로에서 요청마다 스택을 찍으면 로그 I/O 자체가 응답 지연을 밀어 올려, 장애 대응이 아니라
     * 장애 증폭이 된다. 그런 코드만 여기서 {@code false} 로 내린다.
     */
    default boolean logStackTrace() {
        return true;
    }
}

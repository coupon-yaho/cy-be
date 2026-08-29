package com.kafkick.core.observation;

import java.util.Objects;

/** 계약 위반은 숨기지 않으며, A-04가 생성과 기록을 함께 실패-안전화한다. */
public class IssuanceFlowEventFactory {

    private final EventIdGenerator eventIdGenerator;

    public IssuanceFlowEventFactory(EventIdGenerator eventIdGenerator) {
        this.eventIdGenerator = Objects.requireNonNull(eventIdGenerator, "eventIdGenerator");
    }

    public IssuanceFlowEvent issued(
            IssuanceFlowEvent.Ctx context,
            long issuanceId,
            String issuanceCode
    ) {
        return IssuanceFlowEvent.issued(
                eventIdGenerator.generate(), context, issuanceId, issuanceCode
        );
    }

    public IssuanceFlowEvent issueRejected(
            IssuanceFlowEvent.Ctx context,
            int httpStatus,
            ReasonCode reasonCode,
            Dependency dependency
    ) {
        return IssuanceFlowEvent.issueRejected(
                eventIdGenerator.generate(), context, httpStatus, reasonCode, dependency
        );
    }

    public IssuanceFlowEvent entry(
            IssuanceFlowEvent.Ctx context,
            int httpStatus,
            ReasonCode reasonCode,
            Dependency dependency,
            Long queuePosition,
            Long queueSequence
    ) {
        return IssuanceFlowEvent.entry(
                eventIdGenerator.generate(), context, httpStatus, reasonCode,
                dependency, queuePosition, queueSequence
        );
    }

    /**
     * 정책 검증을 통과한 요청이 발급 엔진에 진입한 사실을 이벤트로 만듭니다.
     *
     * <p>결과가 아니라 단계이므로 응답 상태와 발급 식별자를 받지 않습니다.
     *
     * @param context 진입 대상과 실행 설정을 담은 공통 관측 정보
     * @return 발급 시도 단계 이벤트
     */
    public IssuanceFlowEvent issueAttempt(IssuanceFlowEvent.Ctx context) {
        return IssuanceFlowEvent.issueAttempt(eventIdGenerator.generate(), context);
    }

    public IssuanceFlowEvent admitted(IssuanceFlowEvent.Ctx context, long queueSequence) {
        return IssuanceFlowEvent.admitted(eventIdGenerator.generate(), context, queueSequence);
    }
}

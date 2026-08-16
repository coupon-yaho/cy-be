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

    public IssuanceFlowEvent admitted(IssuanceFlowEvent.Ctx context, long queueSequence) {
        return IssuanceFlowEvent.admitted(eventIdGenerator.generate(), context, queueSequence);
    }
}

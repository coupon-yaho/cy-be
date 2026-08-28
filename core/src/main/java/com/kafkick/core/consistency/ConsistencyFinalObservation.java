package com.kafkick.core.consistency;

import java.util.Objects;

import com.kafkick.core.admin.overview.calculator.ConsistencyActionContext;
import com.kafkick.core.observation.SourceStatus;

/** 회차별 최신 FINAL 정합성 결과의 조회 계약입니다. */
public record ConsistencyFinalObservation(
        SourceStatus status,
        ConsistencyActionContext value
) {
    public ConsistencyFinalObservation {
        Objects.requireNonNull(status, "status");
        if (status != SourceStatus.PENDING && status != SourceStatus.VALID
                && status != SourceStatus.UNAVAILABLE && status != SourceStatus.N_A) {
            throw new IllegalArgumentException("FINAL 조회에서 허용하지 않는 상태입니다: " + status);
        }
        if ((status == SourceStatus.VALID) != (value != null)) {
            throw new IllegalArgumentException("VALID FINAL 조회만 값을 가져야 합니다: " + status);
        }
    }
}

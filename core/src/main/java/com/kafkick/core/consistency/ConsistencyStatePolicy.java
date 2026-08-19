package com.kafkick.core.consistency;

import com.kafkick.core.observation.SourceStatus;

import java.util.Set;

/** 정합성 원천과 계산 결과가 공통으로 사용하는 상태 범위를 정의합니다. */
final class ConsistencyStatePolicy {

    private static final Set<SourceStatus> SUPPORTED_STATES = Set.of(
            SourceStatus.VALID,
            SourceStatus.PENDING,
            SourceStatus.STALE,
            SourceStatus.UNAVAILABLE,
            SourceStatus.N_A
    );

    private ConsistencyStatePolicy() {
    }

    /**
     * 지정한 상태를 정합성 값으로 해석할 수 있는지 확인합니다.
     *
     * @param status 확인할 원천 상태
     * @return 정합성 상태 모델이 지원하면 {@code true}
     */
    static boolean isSupported(SourceStatus status) {
        return SUPPORTED_STATES.contains(status);
    }
}

package com.kafkick.core.admin.preparation;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/** V2 예약 회차의 Redis 워밍업·게이트 준비 판정 원천입니다. */
public record V2PreparationSource(
        Boolean warmupReady,
        Boolean gateReady,
        SourceStatus status,
        Instant observedAt
) {

    /** 값 보유 상태와 두 준비 판정·관측 시각의 조합을 검증합니다. */
    public V2PreparationSource {
        Objects.requireNonNull(status, "status");
        if (status == SourceStatus.VALID) {
            if (warmupReady == null || gateReady == null || observedAt == null) {
                throw new IllegalArgumentException(
                        "VALID V2 준비 원천에는 두 판정과 observedAt이 필요합니다.");
            }
        } else {
            if (status != SourceStatus.PENDING
                    && status != SourceStatus.UNAVAILABLE
                    && status != SourceStatus.N_A) {
                throw new IllegalArgumentException("V2 준비 원천에 지원하지 않는 상태입니다: " + status);
            }
            if (warmupReady != null || gateReady != null || observedAt != null) {
                throw new IllegalArgumentException(status + " V2 준비 원천은 값을 가질 수 없습니다.");
            }
        }
    }

    /** V2 준비 판정이 적용되지 않는 회차의 원천을 반환합니다. */
    public static V2PreparationSource notApplicable() {
        return new V2PreparationSource(null, null, SourceStatus.N_A, null);
    }

    /** Redis Reader를 사용할 수 없는 V2 조회 대상의 원천을 반환합니다. */
    public static V2PreparationSource unavailable() {
        return new V2PreparationSource(null, null, SourceStatus.UNAVAILABLE, null);
    }
}

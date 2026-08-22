package com.kafkick.core.consistency;

import com.kafkick.core.observation.SourceStatus;

import java.time.Instant;
import java.util.Objects;

/**
 * 정합성 원천 하나의 수집 상태와 마지막 관측 시각입니다.
 *
 * @param status 원천 수집 상태
 * @param observedAt 실제 값이 관측된 시각; 값이 없는 상태이면 {@code null}
 */
public record SourceObservation(SourceStatus status, Instant observedAt) {

    /** 원천 상태와 관측 시각 조합이 유효한지 검증합니다. */
    public SourceObservation {
        Objects.requireNonNull(status, "status");
        if (status.carriesValue() && observedAt == null) {
            throw new IllegalArgumentException(status + " 상태에는 실제 관측 시각이 필요합니다.");
        }
        if (!status.carriesValue() && observedAt != null) {
            throw new IllegalArgumentException(status + " 상태의 observedAt은 null이어야 합니다.");
        }
    }
}

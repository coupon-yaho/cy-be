package com.kafkick.core.consistency;

import com.kafkick.core.observation.SourceStatus;

import java.time.Instant;
import java.util.Objects;

public record SourceObservation(SourceStatus status, Instant observedAt) {

    public SourceObservation {
        Objects.requireNonNull(status, "status");
        if ((status == SourceStatus.VALID
                || status == SourceStatus.WARMING_UP
                || status == SourceStatus.STALE
                || status == SourceStatus.NO_TRAFFIC) && observedAt == null) {
            throw new IllegalArgumentException(status + " 상태에는 실제 관측 시각이 필요합니다.");
        }
        if ((status == SourceStatus.PENDING
                || status == SourceStatus.UNAVAILABLE
                || status == SourceStatus.N_A) && observedAt != null) {
            throw new IllegalArgumentException(status + " 상태의 observedAt은 null이어야 합니다.");
        }
    }
}

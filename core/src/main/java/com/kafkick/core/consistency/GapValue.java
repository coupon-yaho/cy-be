package com.kafkick.core.consistency;

import com.kafkick.core.observation.SourceStatus;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

public record GapValue(Long value, SourceStatus state, Instant observedAt) {

    private static final Set<SourceStatus> ALLOWED_STATES = EnumSet.of(
            SourceStatus.VALID,
            SourceStatus.PENDING,
            SourceStatus.STALE,
            SourceStatus.UNAVAILABLE,
            SourceStatus.N_A
    );

    public GapValue {
        Objects.requireNonNull(state, "state");
        if (!ALLOWED_STATES.contains(state)) {
            throw new IllegalArgumentException("GapValue에서 허용하지 않는 상태입니다: " + state);
        }
        if ((state == SourceStatus.VALID || state == SourceStatus.STALE)
                && (value == null || observedAt == null)) {
            throw new IllegalArgumentException(state + " 상태에는 value와 observedAt이 필요합니다.");
        }
        if ((state == SourceStatus.PENDING
                || state == SourceStatus.UNAVAILABLE
                || state == SourceStatus.N_A) && value != null) {
            throw new IllegalArgumentException(state + " 상태의 value는 null이어야 합니다.");
        }
        if ((state == SourceStatus.PENDING
                || state == SourceStatus.UNAVAILABLE
                || state == SourceStatus.N_A) && observedAt != null) {
            throw new IllegalArgumentException(state + " 상태의 observedAt은 null이어야 합니다.");
        }
    }
}

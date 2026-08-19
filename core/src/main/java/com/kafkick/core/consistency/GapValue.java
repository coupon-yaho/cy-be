package com.kafkick.core.consistency;

import com.kafkick.core.observation.SourceStatus;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * 계산된 값 하나와 그 값을 신뢰할 수 있는지를 함께 표현합니다.
 *
 * <p>{@link SourceStatus#VALID}과 {@link SourceStatus#STALE}은 값과 관측 시각을 유지합니다.
 * PENDING, UNAVAILABLE, N_A는 숫자 0으로 오인되지 않도록 값과 관측 시각을 비워 둡니다.
 *
 * @param value 계산 결과; 값을 제공할 수 없는 상태이면 {@code null}
 * @param state 계산 결과의 가용성 상태
 * @param observedAt 계산에 사용한 원천 중 가장 오래된 관측 시각
 */
public record GapValue(Long value, SourceStatus state, Instant observedAt) {

    private static final Set<SourceStatus> ALLOWED_STATES = EnumSet.of(
            SourceStatus.VALID,
            SourceStatus.PENDING,
            SourceStatus.STALE,
            SourceStatus.UNAVAILABLE,
            SourceStatus.N_A
    );

    /** 상태와 값·관측 시각 조합이 유효한지 검증합니다. */
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

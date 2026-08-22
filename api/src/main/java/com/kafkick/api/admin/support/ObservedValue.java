package com.kafkick.api.admin.support;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.SourceStatus;

/**
 * 독립 수집 원천의 값, 수집 상태, 마지막 관측 시각을 함께 전달하는 공통 관리자 응답 값입니다.
 *
 * <p>{@code value}는 미수집·장애·해당 없음 상태에서 null일 수 있으며, 이를 0이나 정상값으로 대체하면 안 됩니다.
 * {@code observedAt} 역시 실제 관측이 없으면 null입니다. 화면은 값만 보지 않고 {@code state}를 함께 사용해
 * VALID, PENDING, STALE, UNAVAILABLE 등의 상태를 구분해야 합니다.</p>
 *
 * @param <T> 관측한 값의 타입
 * @param value 실제 관측값; 미수집·장애·해당 없음이면 null
 * @param state 값을 제공한 원천의 확정 수집 상태
 * @param observedAt 원천에서 마지막으로 관측한 시각; 관측 이력이 없으면 null
 */
public record ObservedValue<T>(T value, SourceStatus state, Instant observedAt) {

    /**
     * {@code SourceObservation}과 같은 상태·관측 시각 불변식을 HTTP 값 Wrapper에도 적용합니다.
     *
     * <p>VALID·WARMING_UP·STALE·NO_TRAFFIC은 실제 관측값과 시각이 있어야 합니다. PENDING·
     * UNAVAILABLE·N_A는 관측값과 시각이 모두 없어야 하며, 이를 가짜 0이나 조립 시각으로 채우지 않습니다.</p>
     *
     * @throws NullPointerException state가 null인 경우
     * @throws IllegalArgumentException 상태와 value 또는 observedAt 조합이 canonical 관측 규칙을 위반한 경우
     */
    public ObservedValue {
        Objects.requireNonNull(state, "state");
        if (state.carriesValue() && (value == null || observedAt == null)) {
            throw new IllegalArgumentException(state + " 상태에는 value와 observedAt이 필요합니다.");
        }
        if (!state.carriesValue() && (value != null || observedAt != null)) {
            throw new IllegalArgumentException(state + " 상태의 value와 observedAt은 null이어야 합니다.");
        }
    }
}

package com.kafkick.core.consistency;

import com.kafkick.core.observation.Severity;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

/**
 * 한 스냅샷의 정합성 계산 결과입니다.
 *
 * <p>LIVE에서는 {@code verdict}가 없으며 현재 계산 가능한 값이 하나도 없으면
 * {@code severity}도 {@code null}입니다. FINAL에서는 verdict와 severity가 항상 존재합니다.
 *
 * @param gaps 확정된 네 종류의 gap; 키는 항상 모두 포함됨
 * @param overIssued 총 발급 수량을 초과한 활성 쿠폰 수
 * @param phase 평가 단계
 * @param verdict FINAL 합격 여부; LIVE에서는 {@code null}
 * @param severity 현재 최고 심각도; LIVE에서 평가 가능한 값이 없으면 {@code null}
 */
public record ConsistencyEvaluation(
        Map<ConsistencyGapType, GapValue> gaps,
        GapValue overIssued,
        ConsistencyPhase phase,
        Verdict verdict,
        Severity severity
) {

    /** 단계별 필수 필드와 네 종류 gap의 완전성을 검증하고 맵을 불변으로 복사합니다. */
    public ConsistencyEvaluation {
        Objects.requireNonNull(gaps, "gaps");
        Objects.requireNonNull(overIssued, "overIssued");
        Objects.requireNonNull(phase, "phase");
        if (!gaps.keySet().equals(EnumSet.allOf(ConsistencyGapType.class))) {
            throw new IllegalArgumentException("정합성 gap은 확정된 네 종류를 모두 포함해야 합니다.");
        }
        if (gaps.values().stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("정합성 gap 값은 null일 수 없습니다.");
        }
        // null은 평가 불가, NONE은 경보 없음이며 LIVE에서도 현재 severity는 계산할 수 있다.
        if (phase == ConsistencyPhase.LIVE && verdict != null) {
            throw new IllegalArgumentException("LIVE 단계에는 verdict를 지정할 수 없습니다.");
        }
        if (phase == ConsistencyPhase.FINAL && verdict == null) {
            throw new IllegalArgumentException("FINAL 단계에는 verdict가 필요합니다.");
        }
        if (phase == ConsistencyPhase.FINAL && severity == null) {
            throw new IllegalArgumentException("FINAL 단계에는 severity가 필요합니다.");
        }
        Map<ConsistencyGapType, GapValue> orderedGaps = new EnumMap<>(ConsistencyGapType.class);
        orderedGaps.putAll(gaps);
        gaps = Collections.unmodifiableMap(orderedGaps);
    }
}

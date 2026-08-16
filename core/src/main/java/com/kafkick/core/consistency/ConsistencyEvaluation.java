package com.kafkick.core.consistency;

import com.kafkick.core.observation.Severity;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

public record ConsistencyEvaluation(
        Map<ConsistencyGapType, GapValue> gaps,
        GapValue overIssued,
        ConsistencyPhase phase,
        Verdict verdict,
        Severity severity
) {

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

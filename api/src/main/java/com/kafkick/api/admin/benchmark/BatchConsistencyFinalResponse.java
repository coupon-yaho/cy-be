package com.kafkick.api.admin.benchmark;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.GapValue;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/**
 * batch 내부 경계의 응답 모양입니다. batch 모듈 타입을 api가 컴파일 타임에 볼 수 없으므로
 * 같은 계약을 이쪽에도 둡니다 — 두 모양이 갈라지면 양쪽의 계약 테스트가 잡습니다.
 *
 * @param evaluation 성공 시의 FINAL 계산 결과; 거절이면 {@code null}
 * @param violations 거절 사유; 성공이면 빈 목록
 */
public record BatchConsistencyFinalResponse(
        EvaluationPayload evaluation, List<Violation> violations) {

    public BatchConsistencyFinalResponse {
        violations = violations == null ? List.of() : List.copyOf(violations);
    }

    /** batch의 {@code TopologyValidator.Violation}과 같은 모양입니다. */
    public record Violation(String key, String expected, String actual, String reason) {
    }

    /**
     * 도메인 모델을 와이어에서 분리하는 전송 표현입니다. 도메인 불변식은 {@link #toDomain()}
     * 에서 한 번에 강제되므로, 어긋난 본문이 역직렬화 도중이 아니라 변환 지점에서 드러납니다.
     */
    public record EvaluationPayload(
            Map<ConsistencyGapType, GapPayload> gaps,
            GapPayload overIssued,
            ConsistencyPhase phase,
            Verdict verdict,
            Severity severity) {

        public EvaluationPayload {
            gaps = gaps == null ? Map.of() : Map.copyOf(gaps);
        }

        /** @throws IllegalArgumentException batch 응답이 FINAL 계약을 만족하지 않으면 */
        public ConsistencyEvaluation toDomain() {
            Map<ConsistencyGapType, GapValue> domainGaps = new EnumMap<>(ConsistencyGapType.class);
            gaps.forEach((type, gap) -> domainGaps.put(type, gap.toDomain()));
            return new ConsistencyEvaluation(domainGaps,
                    overIssued == null ? null : overIssued.toDomain(),
                    phase, verdict, severity);
        }
    }

    /** {@code GapValue}의 전송 표현입니다. */
    public record GapPayload(Long value, SourceStatus state, Instant observedAt) {

        GapValue toDomain() {
            return new GapValue(value, state, observedAt);
        }
    }
}

package com.kafkick.api.admin.benchmark;

import java.util.List;

import com.kafkick.core.consistency.ConsistencyEvaluation;

/**
 * batch 내부 경계의 응답 모양입니다. batch 모듈 타입을 api가 컴파일 타임에 볼 수 없으므로
 * 같은 계약을 이쪽에도 둡니다 — 두 모양이 갈라지면 양쪽의 계약 테스트가 잡습니다.
 *
 * @param evaluation 성공 시의 FINAL 계산 결과; 거절이면 {@code null}
 * @param violations 거절 사유; 성공이면 빈 목록
 */
public record BatchConsistencyFinalResponse(
        ConsistencyEvaluation evaluation, List<Violation> violations) {

    public BatchConsistencyFinalResponse {
        violations = violations == null ? List.of() : List.copyOf(violations);
    }

    /** batch의 {@code TopologyValidator.Violation}과 같은 모양입니다. */
    public record Violation(String key, String expected, String actual, String reason) {
    }
}

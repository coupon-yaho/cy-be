// 검증 배치 접수 응답입니다.
package com.kafkick.batch.api;

import java.time.LocalDateTime;

import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;

/**
 * <b>{@code executionId} 지 {@code runId} 가 아니다.</b> 후자는 {@code startRunStep} 이
 * 가드 여덟을 통과한 뒤에야 만들므로 접수 시점에 존재하지 않는다. 조회 응답이 그것을 준다.
 *
 * <p>서버가 채운 값을 함께 돌려준다. {@code dataset}·{@code attempt} 는 안 주면 서버가
 * 정하는데, <b>무엇으로 정했는지 안 보이면 요청자가 자기가 뭘 돌렸는지 모른다.</b>
 */
public record TriggerAccepted(
        Long executionId,
        LocalDateTime asOf,
        DatasetType dataset,
        ScopeType scope,
        int attempt
) {
}

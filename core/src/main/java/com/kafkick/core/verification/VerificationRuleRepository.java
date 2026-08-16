// 검증 규칙의 판정 질의 계약입니다. 규칙 하나가 메서드 하나입니다.
package com.kafkick.core.verification;

import java.time.LocalDateTime;
import java.util.List;

/**
 * <b>어긋난 것만 돌려줍니다.</b> 300만 행을 훑되 자바로 올라오는 것은 위반뿐입니다.
 * 정상셋에서는 0건이고 오염셋에서도 규칙당 100건이라, 결과를 메모리에 담아도 됩니다.
 * 스캔이 큰 것이지 산출이 큰 것이 아닙니다.
 *
 * <p>그래서 {@code limit} 을 받습니다. 검증기 자체가 망가지면(예: 시각 비교가 밀리면)
 * 위반이 수백만 건으로 튀는데, 그것을 그대로 담으면 배치가 OOM 으로 죽고 원인이 묻힙니다.
 * 상한에 닿았다는 사실 자체가 <b>데이터가 아니라 검증기를 의심하라</b>는 신호입니다.
 */
public interface VerificationRuleRepository {

    /**
     * V3 리플레이 대조 — 접은 상태와 {@code issuances.status} 가 다른 발급건.
     *
     * <p>오염 유형 2(이력은 USED 인데 저장값은 ISSUED)가 이 규칙에 잡힙니다.
     *
     * <p><b>{@code asOf} 이후에 갱신된 발급건은 비교하지 않습니다.</b> 접힌 상태는 asOf 로 얼어 있는데
     * {@code issuances.status} 는 질의 순간의 현재값이라, 배치가 도는 동안 런타임이 건드린 발급건이
     * 전부 어긋난 것으로 잡힙니다. 정상셋에서 오탐이 나고 재실행 결과도 달라집니다.
     */
    List<VerificationFinding> findReplayMismatches(long runId, LocalDateTime asOf, int limit);

    /**
     * V5 사용 실적 정합 — 접은 상태와 활성 사용 건수가 어긋나는 발급건.
     *
     * <p>불변식은 <b>{@code USED} 면 활성 사용 1건, 아니면 0건</b>입니다.
     * 오염 유형 7(저장값은 ISSUED 인데 활성 사용 행이 남음)이 여기 잡히고,
     * 같은 식이 한 발급건의 이중 사용도 잡습니다.
     *
     * <p>{@code asof_state.active_usage_count} 는 Step 0 이 이미 채웠으므로
     * 이 규칙은 {@code issuance_usages} 를 다시 읽지 않습니다.
     */
    List<VerificationFinding> findUsageMismatches(long runId, int limit);
}

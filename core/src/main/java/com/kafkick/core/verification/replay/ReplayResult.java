// 발급건 하나를 접은 결과입니다. asof_state 한 행과 V4 위반 목록이 함께 나옵니다.
package com.kafkick.core.verification.replay;

import java.time.LocalDateTime;
import java.util.List;

import com.kafkick.core.coupon.IssuanceStatus;

/**
 * {@code asof_state} 행은 여기에 {@code active_usage_count} 를 더해 완성됩니다.
 * 사용 건수는 {@code issuance_usages} 에서 오므로 접기 단계가 알 수 없습니다.
 *
 * <p>{@code asof_state.coupon_id} 는 레거시 컬럼명이고 실제로 담기는 값은
 * {@code issuances.id} 입니다. 그래서 여기서는 {@code issuanceId} 로 부릅니다.
 */
public record ReplayResult(
        long issuanceId,
        IssuanceStatus state,
        long lastHistoryId,
        LocalDateTime lastEventAt,
        List<IllegalTransition> illegalTransitions
) {

    public ReplayResult {
        if (state == null) {
            throw new IllegalArgumentException("리플레이 결과 상태가 필요합니다.");
        }
        if (lastEventAt == null) {
            throw new IllegalArgumentException("마지막 이력 시각이 필요합니다.");
        }
        illegalTransitions = List.copyOf(illegalTransitions);
    }

    /** 이 발급건이 V4 를 울렸는가. */
    public boolean hasIllegalTransition() {
        return !illegalTransitions.isEmpty();
    }
}

// 한 발급건의 이력 묶음입니다. 리더가 내보내는 단위이고 접기의 입력입니다.
package com.kafkick.batch.replay;

import java.util.List;

import com.kafkick.core.verification.replay.IssuanceHistoryRecord;

/**
 * 발급건 하나의 이력이 <b>전부</b> 들어 있어야 합니다. 일부만 들어오면 접은 상태가
 * 중간값으로 굳고, 그 발급건은 실제와 다른 상태로 {@code asof_state} 에 남습니다.
 */
public record IssuanceHistoryGroup(long issuanceId, List<IssuanceHistoryRecord> histories) {

    public IssuanceHistoryGroup {
        if (histories == null || histories.isEmpty()) {
            throw new IllegalArgumentException("이력이 없는 묶음입니다. 발급건=" + issuanceId);
        }
        histories = List.copyOf(histories);
    }
}

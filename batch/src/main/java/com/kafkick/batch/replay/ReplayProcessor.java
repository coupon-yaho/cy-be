// 이력 묶음을 접어 asOf 시점 상태로 바꿉니다. 판정 로직은 전부 core 에 있습니다.
package com.kafkick.batch.replay;

import org.springframework.batch.infrastructure.item.ItemProcessor;

import com.kafkick.core.verification.replay.HistoryReplay;
import com.kafkick.core.verification.replay.ReplayResult;

/**
 * 접기 자체는 {@link HistoryReplay} 가 합니다. 런타임과 검증이 같은 전이표를 쓰게 하려면
 * 판정이 배치 안에 있으면 안 됩니다.
 *
 * <p>결과에 딸려 나오는 불법 전이는 여기서 버리지 않고 그대로 들고 갑니다.
 * V4 가 이력을 다시 접지 않고 이 결과를 받아쓸 수 있어야 합니다.
 */
public class ReplayProcessor implements ItemProcessor<IssuanceHistoryGroup, ReplayResult> {

    @Override
    public ReplayResult process(IssuanceHistoryGroup group) {
        return HistoryReplay.fold(group.issuanceId(), group.histories());
    }
}

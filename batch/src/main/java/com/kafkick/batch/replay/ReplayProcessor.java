// 이력 묶음을 접어 asOf 시점 상태로 바꿉니다. 판정 로직은 전부 core 에 있습니다.
package com.kafkick.batch.replay;

import org.springframework.batch.infrastructure.item.ItemProcessor;

import com.kafkick.core.verification.replay.HistoryReplay;
import com.kafkick.core.verification.replay.ReplayResult;

/**
 * 접기 자체는 {@link HistoryReplay} 가 합니다. 런타임과 검증이 같은 전이표를 쓰게 하려면
 * 판정이 배치 안에 있으면 안 됩니다.
 *
 * <p><b>결과에 딸려 나오는 불법 전이(V4)는 아직 어디에도 저장되지 않습니다.</b>
 * 청크가 써지면 그대로 사라집니다. 저장은 규칙 티켓의 몫입니다.
 *
 * <p>그때 V4 를 <b>별도 Step 으로 만들면 안 됩니다.</b> 그러면 이력 534만 행을 다시 접어야 하고,
 * 접기 구현이 두 벌로 갈라져 {@code asof_state} 와 V4 가 서로 다른 말을 하게 됩니다.
 * 이 Step 의 라이터를 늘려 같은 청크 트랜잭션에서 함께 쓰는 것이 맞습니다.
 */
public class ReplayProcessor implements ItemProcessor<IssuanceHistoryGroup, ReplayResult> {

    @Override
    public ReplayResult process(IssuanceHistoryGroup group) {
        return HistoryReplay.fold(group.issuanceId(), group.histories());
    }
}

// 접기 결과를 asof_state 로 흘려보냅니다. 저장 SQL 은 storage 어댑터가 압니다.
package com.kafkick.batch.replay;

import java.util.List;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;

import com.kafkick.core.verification.replay.AsOfStateRepository;
import com.kafkick.core.verification.replay.ReplayResult;

/**
 * 람다로 두지 않고 클래스로 둡니다. {@code @StepScope} 빈은 CGLIB 프록시로 감싸이는데
 * 람다가 만드는 클래스는 final 이라 프록시할 수 없습니다.
 */
public class AsOfStateItemWriter implements ItemWriter<ReplayResult> {

    private final AsOfStateRepository asOfStates;
    private final long runId;

    public AsOfStateItemWriter(AsOfStateRepository asOfStates, long runId) {
        this.asOfStates = asOfStates;
        this.runId = runId;
    }

    @Override
    public void write(Chunk<? extends ReplayResult> chunk) {
        asOfStates.appendAll(runId, List.copyOf(chunk.getItems()));
    }
}

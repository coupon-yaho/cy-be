// 접기가 낸 불법 전이를 V4 검출로 남깁니다. asof_state 와 같은 청크 트랜잭션에서 씁니다.
package com.kafkick.batch.replay;

import java.util.List;

import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;

import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.replay.IllegalTransition;
import com.kafkick.core.verification.replay.ReplayResult;

/**
 * <b>V4 를 별도 Step 으로 만들지 않는 이유입니다.</b> 별도 Step 은 이력 534만 행을 다시 접어야
 * 하고, 그러면 접기 구현이 두 벌로 갈라져 {@code asof_state} 와 V4 가 서로 다른 말을 하게 됩니다.
 * 접기는 한 번만 돌고 산출물 두 개가 같은 청크에서 나갑니다.
 *
 * <p>같은 트랜잭션이라 <b>둘이 함께 커밋되거나 함께 되돌아갑니다.</b> 갈라지면 재시작 뒤에
 * {@code asof_state} 는 있는데 V4 는 없는 구간이 생깁니다.
 *
 * <p>이력 한 행은 위반을 많아야 하나 냅니다({@link IllegalTransition}). 그래서 여기서
 * 중복 제거를 하지 않아도 {@code uk_run_finding} 과 어긋나지 않습니다.
 */
public class IllegalTransitionItemWriter implements ItemWriter<ReplayResult> {

    private final VerificationFindingRepository findings;
    private final long runId;

    public IllegalTransitionItemWriter(VerificationFindingRepository findings, long runId) {
        this.findings = findings;
        this.runId = runId;
    }

    @Override
    public void write(Chunk<? extends ReplayResult> chunk) {
        List<VerificationFinding> detected = chunk.getItems().stream()
                .map(ReplayResult::illegalTransitions)
                .flatMap(List::stream)
                .map(IllegalTransitionItemWriter::toFinding)
                .toList();

        findings.appendAll(runId, detected);
    }

    private static VerificationFinding toFinding(IllegalTransition illegal) {
        return VerificationFinding.forHistory(
                FindingType.ILLEGAL_TRANSITION,
                illegal.historyId(),
                illegal.expected(),
                illegal.actual());
    }
}

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
 *
 * <p><b>누적 상한을 둡니다.</b> 규칙 셋 중 모수가 가장 큽니다 — V3·V5 는 발급건 300만인데
 * 여기는 이력 534만이고, 전 행이 자바 객체를 통과하는 유일한 규칙입니다. 전이표에서 한 줄만
 * 빠져도 그 사건의 이력이 <b>전부</b> 위반이 되어 수백만 행이 쌓이고, 그러면 실패가
 * "검증기 고장" 이 아니라 "데이터가 수백만 건 깨졌다" 로 보입니다.
 *
 * <p>재시작하면 카운터가 0부터 다시 셉니다. 상한의 목적이 폭주 감지이지 정확한 총계가 아닙니다.
 */
public class IllegalTransitionItemWriter implements ItemWriter<ReplayResult> {

    private final VerificationFindingRepository findings;
    private final long runId;
    private final int maxFindings;

    private long written;

    public IllegalTransitionItemWriter(
            VerificationFindingRepository findings, long runId, int maxFindings) {
        if (maxFindings < 1) {
            throw new IllegalArgumentException("검출 상한은 1 이상이어야 합니다. 값=" + maxFindings);
        }

        this.findings = findings;
        this.runId = runId;
        this.maxFindings = maxFindings;
    }

    @Override
    public void write(Chunk<? extends ReplayResult> chunk) {
        List<VerificationFinding> detected = chunk.getItems().stream()
                .map(ReplayResult::illegalTransitions)
                .flatMap(List::stream)
                .map(IllegalTransitionItemWriter::toFinding)
                .toList();

        // 쓰기 전에 센다. 규칙 Step 은 limit + 1 을 요청해 넘침을 쓰기 전에 판정하는데,
        // 여기만 쓰고 나서 던지면 폭주한 만큼을 일단 DB 에 밀어 넣게 된다.
        written += detected.size();
        if (written > maxFindings) {
            throw new IllegalStateException(
                    "V4 검출이 상한에 닿았습니다. 전이표를 의심하십시오. 상한=" + maxFindings
                            + " 누적=" + written);
        }

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

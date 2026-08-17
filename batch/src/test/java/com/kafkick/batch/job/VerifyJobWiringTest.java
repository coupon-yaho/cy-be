// Step 체인이 "결정론 규칙 먼저" 규율을 지키는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import com.kafkick.core.verification.FindingType;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code FindingType.isDeterministic()} 을 실제로 부르는 유일한 자리입니다.</b>
 * 그 플래그가 선언한 규율은 {@code FindingType} javadoc 에 있습니다 —
 * <i>"결정론 규칙이 먼저 돌아야 폭주로 중단돼도 결정론적 부분은 이미 확보됩니다"</i>.
 *
 * <p>그런데 그것을 아무도 호출하지 않아, 규율을 어겨도 <b>컴파일러도 테스트도 아무 말을 안 했습니다.</b>
 * 실제로 V2 를 붙이면서 결정론 규칙을 비결정론 규칙 뒤에 배선했고, 같은 커밋의 javadoc 은
 * "앞 구간에 둔다" 라고 반대를 적었습니다. 사람이 두 곳을 맞추는 방식이 실패한 자리입니다.
 *
 * <p>규칙이 늘면 아래 표에 한 줄을 더하게 됩니다. 그 자체가 <b>"순서를 다시 보라"</b> 는 신호입니다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false"
})
@Import(MySqlContainerConfig.class)
class VerifyJobWiringTest {

    /**
     * <b>표를 여기서 또 만들지 않는다.</b> 프로덕션의 {@link VerifyJobConfig#RULE_STEP_TYPES} 를
     * 그대로 쓴다 — 테스트가 자기 표를 들면 그것이 낡는 순간 검사가 조용히 줄어든다.
     * 그쪽은 {@code ruleStep} 진입부에서 등록을 강제하므로 <b>표에 없는 규칙 Step 은 기동에서 죽는다.</b>
     */
    private static final Map<String, FindingType> RULE_STEPS = VerifyJobConfig.RULE_STEP_TYPES;

    /** 규칙이 아닌 Step. 새 Step 은 반드시 둘 중 하나에 등록해야 한다. */
    private static final Set<String> INFRA_STEPS =
            Set.of("startRunStep", "replayStep", "usageCountStep",
                    "assertFrozenStep", "finalizeRunStep");

    @Autowired
    private Job verifyJob;

    private List<String> stepNames() {
        return new ArrayList<>(((SimpleJob) verifyJob).getStepNames());
    }

    @Test
    @DisplayName("결정론 규칙이 전부 비결정론 규칙보다 앞에 있다")
    void runDeterministicRulesFirst() {
        List<String> ordered = stepNames().stream().filter(RULE_STEPS::containsKey).toList();

        int lastDeterministic = -1;
        int firstCurrentRow = Integer.MAX_VALUE;
        for (int i = 0; i < ordered.size(); i++) {
            if (RULE_STEPS.get(ordered.get(i)).isDeterministic()) {
                lastDeterministic = i;
            } else {
                firstCurrentRow = Math.min(firstCurrentRow, i);
            }
        }

        assertThat(lastDeterministic)
                .as("현재 순서 %s — 결정론 규칙이 비결정론 뒤에 있으면 폭주로 중단될 때 "
                        + "확보할 수 있었던 산출을 잃는다", ordered)
                .isLessThan(firstCurrentRow);
    }

    /**
     * <b>이름 규칙으로 추측하지 않는다.</b> 접미사로 규칙 Step 을 골라내면 그 규칙을 안 따르는
     * 이름이 들어왔을 때 필터에 안 걸려 <b>두 테스트 다 초록인 채로 순서 위반이 묻힌다.</b>
     * 분류되지 않은 Step 이 하나라도 있으면 실패시킨다.
     */
    @Test
    @DisplayName("모든 Step 이 규칙 또는 인프라로 분류돼 있다")
    void classifyEveryStep() {
        List<String> unclassified = stepNames().stream()
                .filter(name -> !INFRA_STEPS.contains(name))
                .filter(name -> !RULE_STEPS.containsKey(name))
                .toList();

        assertThat(unclassified)
                .as("규칙도 인프라도 아닌 Step — 분류를 안 하면 순서 검사가 이것을 못 본다")
                .isEmpty();
    }

    /**
     * 반대 방향도 본다. 표에만 있고 체인에 없는 Step 은 <b>배선에서 빠진 것</b>인데,
     * 한 방향 검사로는 더 작은 집합이 통과해 버린다.
     */
    @Test
    @DisplayName("표에 있는 규칙 Step 이 전부 체인에 배선돼 있다")
    void wireEveryRuleStep() {
        assertThat(stepNames())
                .as("표에 있는데 체인에 없다 — 그 규칙은 아예 안 돈다")
                .containsAll(RULE_STEPS.keySet());
    }

    /**
     * V4 를 뺀 다섯 검출이 전부 배선돼 있는가. V4({@code ILLEGAL_TRANSITION})는 규칙 Step 이
     * 아니라 Step 0 의 청크 라이터라 여기 없는 것이 맞다.
     */
    @Test
    @DisplayName("V4 를 뺀 검출 다섯이 모두 규칙 Step 을 갖는다")
    void coverEveryFindingType() {
        assertThat(RULE_STEPS.values())
                .containsExactlyInAnyOrderElementsOf(
                        EnumSet.complementOf(EnumSet.of(FindingType.ILLEGAL_TRANSITION)));
    }

    /**
     * <b>얼림 확인은 규칙 전부보다 뒤, 판정보다 앞이다.</b> 현재 행을 읽는 규칙이 끝난 뒤에
     * 봐야 그 사이 갱신을 잡고, 얼림이 깨진 실행에 판정을 남기면 나중에 그 행만 보고
     * 합격·불합격을 읽게 된다.
     */
    @Test
    @DisplayName("얼림 확인 다음이 판정이고, 판정이 마지막이다")
    void assertFrozenThenFinalize() {
        List<String> names = stepNames();

        assertThat(names).last().isEqualTo("finalizeRunStep");
        assertThat(names.indexOf("assertFrozenStep"))
                .as("얼림이 깨진 실행에 판정을 남기면 안 된다")
                .isLessThan(names.indexOf("finalizeRunStep"));
        assertThat(RULE_STEPS.keySet())
                .allSatisfy(rule -> assertThat(names.indexOf(rule))
                        .as("%s 가 얼림 확인보다 뒤에 있다", rule)
                        .isLessThan(names.indexOf("assertFrozenStep")));
    }

    @Test
    @DisplayName("실행 기록이 항상 처음이다 — asof_state 가 run_id 를 FK 로 문다")
    void startRunFirst() {
        assertThat(stepNames()).first().isEqualTo("startRunStep");
    }
}

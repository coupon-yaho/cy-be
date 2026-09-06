package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.DomainMeterNames;

/**
 * 정합성 알림 규칙이 <b>실제로 나가는 메트릭 이름</b>을 쓰는지 대조한다.
 *
 * <p><b>이 실패는 조용하다.</b> 규칙이 없는 이름을 보면 Prometheus 는 에러가 아니라 빈
 * 결과를 돌려주고, 알림은 영원히 안 뜬다 — 그리고 알림이 안 오는 것은 <b>"사고가 없다"
 * 와 구분되지 않는다.</b> {@link OutboxAlertRuleContractTest} 가 같은 자리를 지키고,
 * 이 클래스는 정합성 쪽을 맡는다.
 *
 * <p><b>여기서는 그 대가가 특히 크다.</b> 이 규칙들이 보는 것은
 * {@code app.consistency.over.issued} — {@code DomainMeterNames} 가 <i>"0 보다 크면 팔지
 * 않은 재고를 판 것"</i> 이라고 적어 둔 값이다. 이름이 갈리면 <b>초과발급이 나도 아무 일도
 * 일어나지 않고</b>, 다음으로 그것을 볼 수 있는 것은 하루 한 번 도는 검증 배치뿐이다.
 *
 * <p>Micrometer 는 점을 밑줄로 바꾸고 <b>카운터에만</b> {@code _total} 을 붙인다.
 * 이 셋은 전부 게이지라 안 붙는다.
 */
class ConsistencyAlertRuleContractTest {

    private static final Path RULES = Path.of("../infra/prometheus/rules/consistency-alerts.yml");

    @Test
    @DisplayName("규칙이 쓰는 메트릭 이름이 DomainMeterNames 의 상수와 같다")
    void alertRulesReferenceMetersThatActuallyExist() throws Exception {
        String rules = Files.readString(RULES, StandardCharsets.UTF_8);

        assertNamesExactly(rules, gaugeName(DomainMeterNames.OVER_ISSUED),
                "초과발급 게이지 — 이 이름이 갈리면 초과발급이 나도 안 뜬다");
    }

    /**
     * <b>대응 절차가 가리키는 이름도 실재해야 한다.</b> 알림 본문이 "이것도 함께 보라" 고
     * 적은 지표가 없는 이름이면, 사람이 그 이름으로 검색하다 <b>빈 화면을 보고 자기가
     * 잘못 봤다고 생각한다.</b>
     */
    @Test
    @DisplayName("알림 본문이 가리키는 지표 이름도 실재한다")
    void theRunbookNamesAlsoExist() throws Exception {
        String rules = Files.readString(RULES, StandardCharsets.UTF_8);

        assertNamesExactly(rules, gaugeName(DomainMeterNames.OVER_ISSUED_STATE), "NaN 의 이유를 답하는 상태 미터");
        assertNamesExactly(rules, gaugeName(DomainMeterNames.CONSISTENCY_COUPON_ID), "관측 대상 회차를 싣는 미터 — 회차는 라벨이 아니라 값이다");
        assertNamesExactly(rules, gaugeName(DomainMeterNames.CONSISTENCY_GAP), "어느 쪽이 어긋났는지 가르는 gap 미터");
    }

    /**
     * <b>NaN 갈래가 살아 있어야 한다.</b> {@code > 0} 은 NaN 에서 거짓이라, 못 재는 구간에는
     * 초과발급 알림이 <b>조용해진다</b> — 그 구간을 따로 잡는 규칙이 사라지면 침묵이
     * 정상처럼 보인다. 자기비교({@code != })가 그 수법이고 {@code absent()} 로는 못 잡는다
     * (계열은 있다).
     */
    @Test
    @DisplayName("못 재는 구간을 잡는 자기비교 규칙이 있다")
    void theUnmeasurableBranchIsStillThere() throws Exception {
        String rules = Files.readString(RULES, StandardCharsets.UTF_8);
        String gauge = gaugeName(DomainMeterNames.OVER_ISSUED);

        assertThat(rules)
                .as("NaN != NaN 이 참인 것을 쓴다 — absent() 로는 계열이 있는 NaN 을 못 잡는다")
                .contains(gauge + " != " + gauge);
        assertThat(rules)
                .as("계열 부재는 따로 잡는다")
                .contains("absent(" + gauge + ")");
    }

    private static String gaugeName(String meterName) {
        return meterName.replace('.', '_');
    }

    /**
     * <b>부분문자열로 보면 안 된다.</b> {@code app_consistency_over_issued} 를
     * {@code contains} 로 찾으면 {@code app_consistency_over_issued_v2} 로 바꾼 규칙도
     * 통과한다 — 이름을 바꾸는 것이 정확히 이 테스트가 잡아야 할 사고인데 그때 조용하다
     * (돌연변이로 확인했다).
     *
     * <p>그래서 <b>뒤에 이름 글자가 안 오는지</b>까지 본다. {@code _state} 처럼 접미어가
     * 붙은 다른 미터는 <b>그 자체로</b> 따로 확인한다.
     */
    private static void assertNamesExactly(String rules, String gauge, String as) {
        assertThat(java.util.regex.Pattern.compile(
                        java.util.regex.Pattern.quote(gauge) + "(?![A-Za-z0-9_])")
                .matcher(rules).find())
                .as(as)
                .isTrue();
    }
}

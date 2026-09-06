package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

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
 * 일어나지 않는다.</b>
 *
 * <h2>원문이 아니라 <b>YAML 구조</b>를 본다</h2>
 *
 * <p>⚠️ 첫 판은 파일 원문에서 이름을 찾았다. 그러면 <b>주석에 남은 이름이 근거가 된다</b> —
 * {@code expr} 의 이름만 바꿔도 파일 상단 주석에 옛 이름이 있으면 통과한다(리뷰가 짚었다).
 * 그래서 {@code expr} 은 {@code expr} 에서, 대응 절차의 이름은 {@code description} 에서
 * 각각 찾는다.
 *
 * <p>Micrometer 는 점을 밑줄로 바꾸고 <b>카운터에만</b> {@code _total} 을 붙인다.
 * 이 셋은 전부 게이지라 안 붙는다.
 */
class ConsistencyAlertRuleContractTest {

    private static final Path RULES = Path.of("../infra/prometheus/rules/consistency-alerts.yml");

    /**
     * <b>알림 이름 → {@code expr}.</b> 주석은 안 들어오고, <b>다른 알림의 표현식도 안
     * 섞인다.</b>
     *
     * <p>⚠️ 두 번 틀렸다. 처음엔 파일 원문에서 찾아 <b>주석에 남은 이름이 근거</b>가 됐고,
     * 고친 뒤에도 {@code expr} 을 통째로 이어 붙여서 <b>다른 규칙이 들고 있는 이름</b>이
     * 근거가 됐다 — 한 알림의 표현식만 깨뜨리는 돌연변이가 그대로 통과했다.
     * <b>알림마다 자기 표현식으로만 판정한다.</b>
     */
    @SuppressWarnings("unchecked")
    private static Map<String, String> expressionsByAlert() throws Exception {
        Map<String, String> byAlert = new LinkedHashMap<>();
        try (InputStream yaml = Files.newInputStream(RULES)) {
            Map<String, Object> root = new Yaml().load(yaml);
            for (Map<String, Object> group : (List<Map<String, Object>>) root.get("groups")) {
                for (Map<String, Object> rule : (List<Map<String, Object>>) group.get("rules")) {
                    byAlert.put(rule.get("alert").toString(), rule.get("expr").toString());
                }
            }
        }
        assertThat(byAlert).as("규칙 파일 구조가 바뀌었다").isNotEmpty();
        return byAlert;
    }

    /** 알림 이름 → {@code annotations.description}. */
    @SuppressWarnings("unchecked")
    private static String runbooks() throws Exception {
        List<String> found = new ArrayList<>();
        try (InputStream yaml = Files.newInputStream(RULES)) {
            Map<String, Object> root = new Yaml().load(yaml);
            for (Map<String, Object> group : (List<Map<String, Object>>) root.get("groups")) {
                for (Map<String, Object> rule : (List<Map<String, Object>>) group.get("rules")) {
                    Map<String, Object> annotations =
                            (Map<String, Object>) rule.get("annotations");
                    found.add(annotations.get("description").toString());
                }
            }
        }
        assertThat(found).as("대응 절차가 하나도 없다").isNotEmpty();
        return String.join("\n", found);
    }

    private static String exprOf(Map<String, String> byAlert, String alert) {
        assertThat(byAlert).as("알림 %s 가 사라졌다", alert).containsKey(alert);
        return byAlert.get(alert);
    }

    @Test
    @DisplayName("규칙이 쓰는 메트릭 이름이 DomainMeterNames 의 상수와 같다")
    void alertRulesReferenceMetersThatActuallyExist() throws Exception {
        Map<String, String> byAlert = expressionsByAlert();

        assertNamesExactly(exprOf(byAlert, "OverIssuanceDetected"),
                gaugeName(DomainMeterNames.OVER_ISSUED),
                "초과발급 알림 자신의 표현식이 그 게이지를 봐야 한다");
    }

    /**
     * <b>대응 절차가 가리키는 이름도 실재해야 한다.</b> 알림 본문이 "이것도 함께 보라" 고
     * 적은 지표가 없는 이름이면, 사람이 그 이름으로 검색하다 <b>빈 화면을 보고 자기가
     * 잘못 봤다고 생각한다.</b>
     */
    @Test
    @DisplayName("알림 본문이 가리키는 지표 이름도 실재한다")
    void theRunbookNamesAlsoExist() throws Exception {
        String runbooks = runbooks();

        assertNamesExactly(runbooks, gaugeName(DomainMeterNames.OVER_ISSUED_STATE),
                "NaN 의 이유를 답하는 상태 미터");
        assertNamesExactly(runbooks, gaugeName(DomainMeterNames.CONSISTENCY_COUPON_ID),
                "관측 대상 회차를 싣는 미터 — 회차는 라벨이 아니라 값이다");
        assertNamesExactly(runbooks, gaugeName(DomainMeterNames.CONSISTENCY_GAP),
                "어느 쪽이 어긋났는지 가르는 gap 미터");
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
        Map<String, String> byAlert = expressionsByAlert();
        String gauge = gaugeName(DomainMeterNames.OVER_ISSUED);

        assertThat(exprOf(byAlert, "ConsistencyUnmeasurable"))
                .as("NaN != NaN 이 참인 것을 쓴다 — absent() 로는 계열이 있는 NaN 을 못 잡는다")
                .contains(gauge + " != " + gauge);
        assertThat(exprOf(byAlert, "ConsistencyGaugeMissing"))
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
        assertThat(Pattern.compile(
                        Pattern.quote(gauge) + "(?![A-Za-z0-9_])")
                .matcher(rules).find())
                .as(as)
                .isTrue();
    }
}

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
 * attempt 알림 규칙이 <b>실제로 나가는 메트릭 이름</b>을 쓰는지 대조한다.
 *
 * <p><b>이 실패는 조용하다.</b> 규칙이 없는 이름을 보면 Prometheus 는 에러가 아니라 빈
 * 결과를 돌려주고, 알림은 영원히 안 뜬다 — 그리고 알림이 안 오는 것은 <b>"사고가 없다"
 * 와 구분되지 않는다.</b>
 *
 * <p><b>여기서는 특히 그렇다.</b> 이 규칙들이 보는 셋은 전부 <b>다른 신호가 없는</b>
 * 사건이다 — 계약 위반은 offset 을 넘겨 버려서 컨슈머가 멀쩡해 보이고, live 쓰기 실패는
 * <b>삼켜지고</b>, 토픽 미반영은 브로커가 대신 만들어 줘서 발급이 그냥 돈다.
 * 이 알림이 안 뜨면 <b>아무 데서도 안 뜬다.</b>
 *
 * <h2>알림별로 묶는다</h2>
 *
 * <p>⚠️ {@code ConsistencyAlertRuleContractTest} 가 이 자리에서 <b>두 번</b> 헐거웠다 —
 * 파일 원문에서 찾으면 <b>주석에 남은 이름</b>이 근거가 되고, {@code expr} 을 통째로 이어
 * 붙이면 <b>다른 규칙이 들고 있는 이름</b>이 근거가 된다. 처음부터 알림별로 묶는다.
 */
class AttemptAlertRuleContractTest {

    private static final Path RULES = Path.of("../infra/prometheus/rules/attempt-alerts.yml");

    @Test
    @DisplayName("규칙이 쓰는 메트릭 이름이 DomainMeterNames 의 상수와 같다")
    void alertRulesReferenceMetersThatActuallyExist() throws Exception {
        Map<String, String> byAlert = expressionsByAlert();

        assertNamesExactly(exprOf(byAlert, "AttemptContractViolations"),
                counterName(DomainMeterNames.ATTEMPT_CONTRACT_VIOLATIONS),
                "버린 레코드를 세는 카운터 — 이 이름이 갈리면 버린 흔적이 아무 데도 안 남는다");
        assertNamesExactly(exprOf(byAlert, "AttemptLiveAppendFailing"),
                counterName(DomainMeterNames.ATTEMPT_LIVE_APPEND_FAILURES),
                "삼켜지는 실패라 이 카운터 말고는 신호가 없다");
        assertNamesExactly(exprOf(byAlert, "KafkaTopicsUnprovisioned"),
                gaugeName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE),
                "토픽 미반영은 발급을 안 막으므로 이 상태 말고는 신호가 없다");
    }

    /**
     * <b>값 미터가 아니라 상태 미터를 봐야 한다.</b> 값은 확인 전에 {@code NaN} 이라
     * {@code != 1} 같은 비교가 <b>NaN 에서 거짓</b>이 되어 조용하다 — 정확히 이 알림이
     * 잡아야 할 구간에서 안 뜬다.
     */
    @Test
    @DisplayName("토픽 알림이 값 미터가 아니라 상태 미터를 본다")
    void theTopicAlertReadsTheStateMeterNotTheValue() throws Exception {
        String expr = exprOf(expressionsByAlert(), "KafkaTopicsUnprovisioned");

        assertNamesExactly(expr, gaugeName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_STATE),
                "상태 미터를 봐야 한다");
        assertThat(Pattern.compile(
                        Pattern.quote(gaugeName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED))
                                + "(?![A-Za-z0-9_])")
                .matcher(expr).find())
                .as("값 미터는 확인 전에 NaN 이라 어떤 비교도 거짓이다 — 그것으로는 못 잡는다")
                .isFalse();
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

        assertNamesExactly(runbooks, counterName(DomainMeterNames.ATTEMPT_LIVE_UNREADABLE),
                "쓰기 실패와 짝인 읽기 실패 — 둘을 함께 봐야 원인이 갈린다");
        assertNamesExactly(runbooks, gaugeName(DomainMeterNames.KAFKA_TOPICS_PROVISIONED_CAUSE),
                "unconfirmed 와 mismatched 는 대응이 다르다");
    }

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

    @SuppressWarnings("unchecked")
    private static String runbooks() throws Exception {
        List<String> found = new ArrayList<>();
        try (InputStream yaml = Files.newInputStream(RULES)) {
            Map<String, Object> root = new Yaml().load(yaml);
            for (Map<String, Object> group : (List<Map<String, Object>>) root.get("groups")) {
                for (Map<String, Object> rule : (List<Map<String, Object>>) group.get("rules")) {
                    found.add(((Map<String, Object>) rule.get("annotations"))
                            .get("description").toString());
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

    /** <b>부분문자열로 보면 안 된다.</b> 이름 뒤에 접미어를 붙인 규칙도 통과한다. */
    private static void assertNamesExactly(String haystack, String name, String as) {
        assertThat(Pattern.compile(Pattern.quote(name) + "(?![A-Za-z0-9_])")
                .matcher(haystack).find())
                .as(as)
                .isTrue();
    }

    /** Micrometer 는 <b>카운터에만</b> {@code _total} 을 붙인다. */
    private static String counterName(String meterName) {
        return meterName.replace('.', '_') + "_total";
    }

    private static String gaugeName(String meterName) {
        return meterName.replace('.', '_');
    }
}

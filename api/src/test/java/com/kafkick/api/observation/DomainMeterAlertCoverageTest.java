package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import com.kafkick.core.observation.DomainMeterNames;

/**
 * <b>모든 도메인 지표가 "알림이 있다" 거나 "일부러 없다" 둘 중 하나여야 한다.</b>
 *
 * <p>CY-932·CY-933 이 알림 공백 셋을 메웠는데, 그때마다 <b>같은 일을 손으로</b> 했다 —
 * 상수를 뽑고, 규칙에서 찾고, 안 걸린 것을 javadoc 으로 읽어 알림 대상인지 판단했다.
 * <b>그 판단이 어디에도 안 남아</b> 다음 사람이 처음부터 다시 한다. 그리고 <b>새 지표가
 * 생겨도 아무도 안 묻는다.</b>
 *
 * <h2>"다 붙여라" 가 아니다</h2>
 *
 * <p>안 붙이는 것도 정당한 결정이다 — {@code *_state} 는 값 미터의 <b>이유</b>이고,
 * {@code app_queue_length} 는 <b>0 이 정상</b>(매진·한가)이며, 안 잰 임계로 알림을 만들면
 * <i>"할 일이 없는 페이지"</i> 가 된다. <b>그 결정을 적게 하는 것</b>이 이 테스트의 목적이다.
 *
 * <p>이 저장소가 이미 쓰는 모양이다 — <i>"코드가 먼저, 테스트가 추가를 강제한다"</i>
 * ({@code FailureSummaryPrefixContractTest} 와 obs 허용목록).
 */
class DomainMeterAlertCoverageTest {

    private static final Path RULES_DIR = Path.of("../infra/prometheus/rules");

    /**
     * <b>일부러 알림을 안 붙인 지표와 그 이유.</b>
     *
     * <p>이유를 <b>자유 문장</b>으로 둔다. enum 으로 가두면 다음 사유가 생길 때 그것부터
     * 고쳐야 하고, 그러면 <b>이유를 사유 목록에 맞춰 쓰게</b> 된다 — 적는 목적이 사라진다.
     *
     * <p>여기서 지우면 테스트가 <b>알림을 요구한다.</b> 반대로 알림을 붙이면 여기서
     * <b>빼야</b> 한다 — 둘 다인 상태를 아래가 막는다.
     */
    private static final Map<String, String> DELIBERATELY_UNALERTED = new LinkedHashMap<>();

    static {
        DELIBERATELY_UNALERTED.put("app.consistency.gap",
                "0 이 아닌 것이 곧 사고가 아니다(집계 시점 차이로 잠깐 벌어진다). 임계를 안 쟀다");
        DELIBERATELY_UNALERTED.put("app.consistency.severity",
                "gap 을 등급으로 접은 값이다. gap 자체의 임계를 안 쟀으므로 그 등급의 임계도 "
                        + "안 잰 것이다. 검증 판정(cy_verification_verdict)은 조용해진 뒤 도는 "
                        + "다른 축이라 이것을 대신하지 않는다 — 실시간 축은 비어 있는 것이 맞다");
        DELIBERATELY_UNALERTED.put("app.consistency.source.skew.seconds",
                "두 원천의 관측 시각 차이. 크기를 함께 실으려고 만든 값이지 임계가 있는 값이 아니다");
        DELIBERATELY_UNALERTED.put("app.queue.length",
                "0 이 정상이다(한가). 임계를 안 쟀다");
        DELIBERATELY_UNALERTED.put("app.coupon.stock.remaining",
                "0 이 정상이다(매진). 임계를 안 쟀다");
        DELIBERATELY_UNALERTED.put("app.coupon.v2.issued.stale.pending",
                "benchmark 회차 전용 수집기다(PendingIssuedGaugeCollector). 상시 운영 축이 아니다");
        DELIBERATELY_UNALERTED.put("app.coupon.v2.issued.corrupt.fields",
                "같은 수집기다. 파손은 WARN 으로 남고, 상시가 아니라 회차 검증에서 본다");
        DELIBERATELY_UNALERTED.put("app.issuance.last.success.epoch",
                "FINAL 진입 게이트(quiet period)를 재는 기준 시각이다. 게이트가 판단을 진다");
        DELIBERATELY_UNALERTED.put("app.observation.coupon.id",
                "식별자다. 임계가 없다 — 다른 알림이 '어느 회차인지' 로 가리킨다");
        DELIBERATELY_UNALERTED.put("app.observation.engine.version",
                "v1/v2 판별자다. 임계가 없다");
        DELIBERATELY_UNALERTED.put("app.attempt.live.sampled",
                "분모다. 실패 쪽(append.failures)이 알림을 진다");
        DELIBERATELY_UNALERTED.put("app.attempt.live.unreadable",
                "쓰기 실패와 짝이라 그쪽 알림 본문이 가리킨다. 혼자서는 원인을 못 가른다");
        DELIBERATELY_UNALERTED.put("app.attempt.archive.outcome",
                "duplicate 가 정상값이라고 javadoc 이 적어 뒀다. 갈래별 임계를 안 쟀다");
        DELIBERATELY_UNALERTED.put("app.outbox.retry.delay",
                "분포다. 종착(app.outbox.dead)이 알림을 진다");
        DELIBERATELY_UNALERTED.put("app.kafka.attempt.publish.failures",
                "삼킨 실패지만 attempt 는 판정 원천이 아니다 — live append 실패가 화면 축을 진다");
        DELIBERATELY_UNALERTED.put("app.kafka.topics.provisioned",
                "값은 확인 전에 NaN 이고 PromQL 에서 NaN != 1 이 참이라 오탐이 된다(실측). "
                        + "상태 미터를 KafkaTopicsUnprovisioned 가 본다");
        DELIBERATELY_UNALERTED.put("app.consistency.coupon.id",
                "관측 대상 회차를 싣는 값이다. 임계가 없고, 다른 알림이 '어느 회차인지' 로 가리킨다");
        DELIBERATELY_UNALERTED.put("app.notify.relay.inflight",
                "이 값 하나로는 한가한 것과 막힌 것이 안 갈린다고 javadoc 이 적어 뒀다 — "
                        + "백로그와 함께 봐야 하고, 알림은 OutboxBacklogGrowing 이 진다");
        DELIBERATELY_UNALERTED.put("app.outbox.retry",
                "되돌려 다시 집는 것은 정상 경로다(발행 실패·대상 소실·lease 만료). "
                        + "사고는 재시도 상한을 넘겨 종착할 때이고 OutboxCommandsDead 가 진다");
    }

    @Test
    @DisplayName("모든 app.* 지표가 알림이 있거나, 없는 이유가 적혀 있다")
    void everyDomainMeterIsEitherAlertedOrExplainedAway() throws IOException {
        String rules = allExpressions();
        List<String> undecided = new ArrayList<>();
        for (String meter : domainMeters()) {
            if (isReferenced(rules, meter) || DELIBERATELY_UNALERTED.containsKey(meter)
                    || isCompanionOfAValueMeter(meter)) {
                continue;
            }
            undecided.add(meter);
        }

        assertThat(undecided)
                .as("""
                        알림을 붙이거나, 왜 안 붙이는지 DELIBERATELY_UNALERTED 에 적으십시오.
                        둘 다 정당한 결정이고, 이 테스트가 요구하는 것은 **결정을 적는 것**입니다.
                        (안 잰 임계로 알림을 만들면 '할 일이 없는 페이지' 가 됩니다)""")
                .isEmpty();
    }

    /**
     * <b>둘 다인 상태를 막는다.</b> 알림을 붙이고도 "일부러 안 붙였다" 가 남아 있으면,
     * 다음 사람이 그 문장을 읽고 <b>없는 줄 안다</b> — 그리고 알림을 하나 더 붙인다.
     */
    @Test
    @DisplayName("알림이 붙은 지표는 '일부러 안 붙였다' 목록에 남아 있지 않다")
    void nothingIsBothAlertedAndExcused() throws IOException {
        String rules = allExpressions();

        assertThat(DELIBERATELY_UNALERTED.keySet().stream().filter(m -> isReferenced(rules, m)))
                .as("알림을 붙였으면 DELIBERATELY_UNALERTED 에서 빼십시오")
                .isEmpty();
    }

    /**
     * 이유가 <b>한 문장은 되어야</b> 목록이 통과용 도장이 안 된다.
     *
     * <p>비어 있지 않은 것만 보면 {@code "-"} 한 글자로도 통과한다. 그렇다고 길이가 뜻을
     * 보장하지도 않으니, <b>기계가 걸 수 있는 최소선</b>으로 {@value #MIN_EXCUSE_LENGTH}자를
     * 둔다 — 그 이상은 사람이 리뷰에서 본다.
     */
    static final int MIN_EXCUSE_LENGTH = 10;

    @Test
    @DisplayName("안 붙인 이유가 최소 " + MIN_EXCUSE_LENGTH + "자를 넘는다 — 한 글자 도장을 막는다")
    void everyExcuseActuallySaysSomething() {
        assertThat(DELIBERATELY_UNALERTED.entrySet())
                .allSatisfy(entry -> assertThat(entry.getValue().strip())
                        .as("%s 의 이유가 %d자를 넘어야 한다 — 짧으면 왜 안 붙였는지가 안 남는다",
                                entry.getKey(), MIN_EXCUSE_LENGTH)
                        .hasSizeGreaterThan(MIN_EXCUSE_LENGTH));
    }

    /** 사라진 지표를 목록이 붙들고 있으면 그 문장이 거짓이 된다. */
    @Test
    @DisplayName("'일부러 안 붙였다' 목록이 실재하는 지표만 가리킨다")
    void theExcuseListDoesNotNameGhosts() {
        assertThat(domainMeters())
                .as("DomainMeterNames 에 없는 이름이 목록에 있다")
                .containsAll(DELIBERATELY_UNALERTED.keySet());
    }

    /**
     * <b>모든 규칙의 {@code expr} 만 모은다.</b>
     *
     * <p>⚠️ 첫 판은 파일 원문을 읽었는데, 그러면 <b>대응 절차에 이름이 나온 것</b>이
     * "알림이 있다" 로 셌다 — 여섯 개가 그렇게 잘못 잡혔다. 알림 본문이 <i>"이것도 함께
     * 보라"</i> 고 가리키는 것과 <b>그 지표를 감시하는 것</b>은 다르다.
     *
     * <p><b>글롭으로 읽는다.</b> 파일 이름을 적으면 새 규칙 파일이 조용히 빠진다 —
     * CI 의 promtool 루프가 같은 이유로 글롭이다.
     */
    @SuppressWarnings("unchecked")
    private static String allExpressions() throws IOException {
        try (Stream<Path> files = Files.list(RULES_DIR)) {
            List<Path> yml = files.filter(p -> p.toString().endsWith(".yml")).sorted().toList();
            assertThat(yml).as("규칙 파일이 하나도 없다 — 경로가 바뀌었는지 확인하라").isNotEmpty();
            List<String> expressions = new ArrayList<>();
            for (Path file : yml) {
                Map<String, Object> root =
                        new Yaml().load(Files.readString(file, StandardCharsets.UTF_8));
                for (Map<String, Object> group : (List<Map<String, Object>>) root.get("groups")) {
                    for (Map<String, Object> rule
                            : (List<Map<String, Object>>) group.get("rules")) {
                        // **기록 규칙(record:)은 알림이 아니다.** 지표를 쓰기만 하는 규칙이
                        // 생기면 "알림이 있다" 로 세어 버린다(리뷰가 짚었다).
                        if (rule.get("alert") == null) {
                            continue;
                        }
                        expressions.add(rule.get("expr").toString());
                    }
                }
            }
            assertThat(expressions).as("규칙이 하나도 없다").isNotEmpty();
            return String.join("\n", expressions);
        }
    }

    /**
     * <b>{@code *.state}·{@code *.cause} 는 값 미터의 짝이다 — 스스로 알림을 지지 않는다.</b>
     *
     * <p>값 미터가 <b>무엇이</b> 를 재고 이 둘이 <b>왜</b> 를 진다. 값 쪽 알림 본문이
     * 이것을 가리키는 것이 이 저장소의 모양이고, 여기에 따로 알림을 달면 같은 사건이
     * 두 번 온다.
     *
     * <p>⚠️ <b>"알림을 달면 안 된다" 는 뜻은 아니다.</b> 값이 NaN 이라 값 미터로는 못 잡는
     * 경우가 있고({@code KafkaTopicsUnprovisioned} 가 그렇다), 그때는 상태 미터가 직접
     * 알림을 진다 — 위 검사가 {@code isReferenced} 를 먼저 보므로 그것도 통과한다.
     * 여기서 면제하는 것은 <b>요구하지 않는다</b> 는 뜻이다.
     */
    private static boolean isCompanionOfAValueMeter(String meter) {
        for (String suffix : List.of(".state", ".cause")) {
            if (!meter.endsWith(suffix)) {
                continue;
            }
            // **짝이 실재하는지까지 본다.** 접미어만 보면, 이 이름을 쓰는 <b>독립</b> 지표가
            // 생겼을 때 짝이 없는데도 조용히 면제된다(리뷰가 짚었다).
            String base = meter.substring(0, meter.length() - suffix.length());
            if (domainMeters().contains(base)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Micrometer 가 이름 뒤에 붙이는 것들.
     *
     * <p><b>게이지는 그대로, 카운터는 {@code _total}, 타이머·분포는
     * {@code _count}·{@code _sum}·{@code _max}·{@code _bucket}</b> 이다.
     * 타이머 계열을 안 보면 <b>알림을 실제로 붙여도 "결정 안 됨" 으로 깨진다</b> —
     * {@code app.outbox.retry.delay} 가 Timer 다(리뷰가 짚었다).
     */
    private static final List<String> METRIC_SUFFIXES =
            List.of("", "_total", "_count", "_sum", "_max", "_bucket");

    private static boolean isReferenced(String rules, String meter) {
        String prom = meter.replace('.', '_');
        return METRIC_SUFFIXES.stream().anyMatch(suffix -> find(rules, prom + suffix));
    }

    private static boolean find(String rules, String name) {
        return Pattern.compile("(?<![A-Za-z0-9_])" + Pattern.quote(name) + "(?![A-Za-z0-9_])")
                .matcher(rules).find();
    }

    /** {@code DomainMeterNames} 의 {@code app.} 으로 시작하는 상수 전부 — 태그는 뺀다. */
    private static List<String> domainMeters() {
        List<String> meters = new ArrayList<>();
        for (Field field : DomainMeterNames.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers()) || field.getType() != String.class) {
                continue;
            }
            try {
                Object value = field.get(null);
                if (value instanceof String name && name.startsWith("app.")) {
                    meters.add(name);
                }
            } catch (IllegalAccessException unreachable) {
                throw new IllegalStateException(field.getName(), unreachable);
            }
        }
        assertThat(meters).as("app.* 상수를 하나도 못 읽었다").isNotEmpty();
        return meters;
    }
}

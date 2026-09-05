package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.DomainMeterNames;

/**
 * outbox 알림 규칙이 <b>실제로 나가는 메트릭 이름</b>을 쓰는지 대조한다.
 *
 * <p><b>이 실패는 조용하다.</b> 규칙이 없는 이름을 보면 Prometheus 는 에러가 아니라 빈
 * 결과를 돌려주고, 알림은 영원히 안 뜬다 — 그리고 알림이 안 오는 것은 <b>"사고가 없다"
 * 와 구분되지 않는다.</b> {@code V2IssuanceAlertRuleContractTest} 가 같은 자리를 지키고,
 * 이 클래스는 outbox 쪽을 맡는다.
 *
 * <p>Micrometer 는 점을 밑줄로 바꾸고 <b>카운터에만</b> {@code _total} 을 붙인다.
 * 게이지에는 안 붙는다 — 규칙이 그 변환을 잘못 쓰면 역시 조용히 빈 결과가 되므로
 * 여기서 둘을 갈라 고정한다.
 */
class OutboxAlertRuleContractTest {

    private static final Path RULES = Path.of("../infra/prometheus/rules/outbox-alerts.yml");

    @Test
    @DisplayName("규칙이 쓰는 메트릭 이름이 DomainMeterNames 의 상수와 같다")
    void alertRulesReferenceMetersThatActuallyExist() throws Exception {
        String rules = Files.readString(RULES, StandardCharsets.UTF_8);

        assertThat(rules)
                .as("종착 카운터")
                .contains(counterName(DomainMeterNames.OUTBOX_DEAD));
        assertThat(rules)
                .as("백로그 게이지 — 게이지에는 _total 이 안 붙는다")
                .contains(gaugeName(DomainMeterNames.OUTBOX_BACKLOG));
    }

    /**
     * <b>대응 절차가 가리키는 이름도 실재해야 한다.</b> 알림 본문이 "이것도 함께 보라" 고
     * 적은 지표가 없는 이름이면, 사람이 그 이름으로 검색하다 <b>빈 화면을 보고 자기가
     * 틀린 줄 안다.</b>
     */
    @Test
    @DisplayName("대응 절차가 가리키는 지표 이름도 실재한다")
    void theRunbookPointsAtRealMeters() throws Exception {
        String rules = Files.readString(RULES, StandardCharsets.UTF_8);

        assertThat(rules).contains(gaugeName(DomainMeterNames.NOTIFY_RELAY_IN_FLIGHT));
        assertThat(rules).contains(counterName(DomainMeterNames.OUTBOX_RETRY));
    }

    @Test
    @DisplayName("모든 알림에 channel 라벨이 있다 — 없으면 sink-unrouted 로 떨어진다")
    void everyAlertDeclaresARoutingChannel() throws Exception {
        String rules = Files.readString(RULES, StandardCharsets.UTF_8);

        long alerts = rules.lines().filter(line -> line.strip().startsWith("- alert:")).count();
        long channels = rules.lines().filter(line -> line.strip().startsWith("channel:")).count();

        assertThat(alerts).isPositive();
        assertThat(channels)
                .as("알림 %d건인데 channel 라벨은 %d건이다", alerts, channels)
                .isEqualTo(alerts);
    }

    /**
     * <b>규칙 동작 시험이 같이 있어야 한다.</b> {@code promtool check rules} 는 문법만 보므로
     * "문법은 맞는데 영원히 안 뜨는" 규칙이 CI 를 통과한다 — batch 쪽이 실제로 두 번
     * 그 상태였다. 시험 파일이 있는지를 여기서 못 박는다.
     */
    @Test
    @DisplayName("규칙 동작 시험 파일이 함께 있다 — 문법 검사만으로는 안 뜨는 규칙을 못 잡는다")
    void theRulesHaveABehaviourTest() {
        assertThat(Path.of("../infra/prometheus/tests/outbox-alerts_test.yml"))
                .as("promtool test rules 가 읽는 파일이다")
                .exists();
    }

    /** 카운터는 점→밑줄에 {@code _total} 이 붙는다. */
    private static String counterName(String meterName) {
        return meterName.replace('.', '_') + "_total";
    }

    /** 게이지는 점→밑줄만. {@code _total} 을 붙이면 없는 이름이 된다. */
    private static String gaugeName(String meterName) {
        return meterName.replace('.', '_');
    }
}

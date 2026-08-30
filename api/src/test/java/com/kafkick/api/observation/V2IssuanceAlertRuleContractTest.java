package com.kafkick.api.observation;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 알림 규칙이 실제로 나가는 메트릭 이름을 쓰는지 대조한다.
 *
 * <p><b>이 실패는 조용하다.</b> 규칙이 없는 이름을 보면 Prometheus 는 에러가 아니라 빈 결과를
 * 돌려주고, 알림은 영원히 안 뜬다. 미터를 갈라 만든 이유가 "기준선 0 이라 임계 없이 잡는다"
 * 인데 이름 하나 어긋나면 그 전부가 무효가 된다 — batch 쪽 {@code BatchMetricExposureTest} 가
 * 같은 사고를 막는 자리와 짝이다.
 *
 * <p>Micrometer 는 점을 밑줄로 바꾸고 카운터에 {@code _total} 을 붙인다. 규칙은 그 변환된
 * 이름을 쓰므로 상수와 문자 그대로 같지 않다 — 그 변환까지 여기서 고정한다.
 */
class V2IssuanceAlertRuleContractTest {

    private static final Path RULES = Path.of("../infra/prometheus/rules/api-alerts.yml");

    @Test
    @DisplayName("규칙이 쓰는 메트릭 이름이 MeterNames 의 상수와 같다")
    void alertRulesReferenceMetersThatActuallyExist() throws Exception {
        String rules = Files.readString(RULES, StandardCharsets.UTF_8);

        assertThat(rules)
                .contains(prometheusName(MeterNames.ISSUANCE_V2_CLAIM_LEAKED))
                .contains(prometheusName(MeterNames.ISSUANCE_V2_DATABASE_MEMBER_DIVERGENCE));
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
     * <b>규칙 파일이 컨테이너 안에 있어야 로드된다.</b> {@code prometheus.yml} 의
     * {@code rule_files} 는 {@code rules/*.yml} 상대 경로라 {@code /etc/prometheus/rules} 를
     * 본다. 설정 파일만 마운트하면 glob 이 0개와 일치하고 <b>에러 없이 규칙 0건으로 뜬다</b> —
     * 실측했다(설정만: {@code /api/v1/rules} 가 {@code groups:[]}, 함께 마운트: 47건).
     *
     * <p>즉 이 배선이 빠지면 이 파일의 알림도, batch 의 45건도 조용히 하나도 안 걸린다.
     * 알림이 안 오는 것은 "사고가 없다"와 구분되지 않아 사람이 알아챌 방법이 없다.
     */
    @Test
    @DisplayName("compose 가 규칙 디렉터리를 Prometheus 에 마운트한다")
    void composeMountsTheRulesDirectoryIntoPrometheus() throws Exception {
        String compose = Files.readString(
                Path.of("../compose.yml"), StandardCharsets.UTF_8);

        assertThat(compose).contains(
                "./infra/prometheus/rules:/etc/prometheus/rules:ro");
    }

    /** Micrometer 의 카운터 이름 변환. 점은 밑줄, 카운터는 {@code _total}. */
    private static String prometheusName(String meterName) {
        return meterName.replace('.', '_') + "_total";
    }
}

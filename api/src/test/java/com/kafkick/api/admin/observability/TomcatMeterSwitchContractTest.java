package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import com.kafkick.api.observation.MeterNames;

/**
 * tomcat 두 행이 값을 내기 위한 <b>두 파일의 계약</b>을 한 테스트에서 잇습니다.
 *
 * <p>규칙표에 이름을 넣는 것과 미터가 등록되는 것은 다른 사건입니다. {@code tomcat.threads.*} 는
 * {@code server.tomcat.mbeanregistry.enabled=true} 일 때만 등록되고, 스위치가 꺼져 있으면 미터가
 * 0 이 되는 게 아니라 <b>아예 없어집니다</b> — 예외도 로그도 없이 두 행만 영원히 빕니다.
 *
 * <p>각각을 따로 보는 테스트 두 개로는 이 계약을 지킬 수 없습니다. 규칙표만 보는 테스트는
 * 스위치를 꺼도 통과하고, yml 만 보는 테스트는 이름이 어긋나도 통과합니다.
 *
 * <p>읽는 대상은 커밋되는 {@code observation.yml.example} 입니다. 실제로 로드되는
 * {@code observation.yml} 은 gitignore 대상이라 신규 클론에 없습니다.
 */
class TomcatMeterSwitchContractTest {

    /** Micrometer 가 이름 뒤에 붙이는 base unit. 이 값이 곧 지표 이름의 접미사다. */
    private static final String THREADS = "threads";

    @Test
    @DisplayName("규칙표의 tomcat 이름과 미터를 켜는 설정 스위치가 함께 서 있다")
    void ruleTableAndRegistrationSwitchAgree() throws IOException {
        assertThat(MetricAggregation.rulesView())
                .containsKeys(MetricAggregation.TOMCAT_BUSY, MetricAggregation.TOMCAT_MAX);

        assertThat(mbeanRegistryEnabled())
                .describedAs("스위치를 끄면 규칙표에 이름이 있어도 tomcat 두 행이 영원히 빈다")
                .isTrue();
    }

    /**
     * 이름의 {@code _threads} 접미사는 우리가 정한 것이 아니라 Micrometer 가 base unit 으로 붙이는
     * 것입니다. 추측하지 않고 <b>실제 레지스트리에 등록해 스크레이프 이름을 재서</b> 확인합니다 —
     * 접미사를 빼면 표본이 0 개인데 예외도 로그도 나지 않습니다.
     */
    @Test
    @DisplayName("Prometheus 이름은 미터 이름에 base unit 을 붙인 것이다")
    void prometheusNameCarriesBaseUnit() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try {
            Gauge.builder(MeterNames.TOMCAT_BUSY, new AtomicInteger(3), AtomicInteger::get)
                    .baseUnit(THREADS)
                    .strongReference(true)
                    .register(registry);

            assertThat(registry.scrape()).contains(MetricAggregation.TOMCAT_BUSY);
        } finally {
            registry.close();
        }
    }

    private static boolean mbeanRegistryEnabled() throws IOException {
        Path example = repoRoot().resolve("api/src/main/resources/observation.yml.example");
        assertThat(example).describedAs("계약에 걸린 파일이 없다").exists();
        try (var in = Files.newInputStream(example)) {
            Map<String, Object> yaml = new Yaml().load(in);
            return Boolean.TRUE.equals(nested(yaml, "server", "tomcat", "mbeanregistry", "enabled"));
        }
    }

    @SuppressWarnings("unchecked")
    private static Object nested(Map<String, Object> root, String... path) {
        Object current = root;
        for (String key : path) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = ((Map<String, Object>) map).get(key);
        }
        return current;
    }

    private static Path repoRoot() {
        Path candidate = Path.of("").toAbsolutePath();
        while (candidate != null) {
            if (Files.isRegularFile(candidate.resolve("settings.gradle"))) {
                return candidate;
            }
            candidate = candidate.getParent();
        }
        throw new IllegalStateException("저장소 루트를 찾지 못했다: " + Path.of("").toAbsolutePath());
    }
}

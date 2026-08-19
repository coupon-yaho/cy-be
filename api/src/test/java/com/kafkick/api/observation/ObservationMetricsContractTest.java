package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;

/**
 * {@code observation.yml} 과 {@link MeterNames} 를 <b>한 테스트 안에서</b> 잇는다.
 *
 * <p>둘을 따로 검증하면 계약을 못 지킨다 — yml 만 보는 테스트는 상수가 바뀌어도 통과하고,
 * 상수만 보는 테스트는 yml 이 바뀌어도 통과한다. 여기서는 타이머를
 * {@link MeterNames#HTTP_SERVER_REQUESTS} 로 <b>등록해서</b> 필터가 실제로 걸리는지 보고,
 * 키 문자열이 상수와 같은지도 따로 본다.
 *
 * <p>읽는 대상은 커밋되는 {@code observation.yml.example} 이다. 실제로 로드되는
 * {@code observation.yml} 과 {@code application.yml} 은 gitignore 대상이라 신규 클론에 없으므로,
 * .example 을 임시 디렉터리에 복사해 {@code spring.config.location} 으로 지목한다.
 *
 * <p>복사해서 진짜로 로드시키는 이유 — 값을 프로퍼티로 주입하면 ConfigData 로딩 경로를
 * 건너뛰어, 정작 이 파일에서만 나는 오류(대괄호 표기 등)를 못 잡는다.
 *
 * <p>레지스트리를 직접 준다. {@code MeterRegistryPostProcessor} 가 {@code MeterFilter} 빈
 * (백분위·expiry 를 거는 {@code PropertiesMeterFilter} 포함)을 <b>모든</b> MeterRegistry 빈에
 * 적용하므로, 운영과 같은 경로를 타면서 시계만 우리 것으로 바꿀 수 있다.
 */
class ObservationMetricsContractTest {

    private static final double P99 = 0.99;

    /**
     * 자동설정 제외는 애노테이션이 아니라 <b>프로퍼티</b>로 준다. 중첩 {@code TestApp} 은
     * {@code com.kafkick} 컴포넌트 스캔에 걸리므로, 여기에
     * {@code @EnableAutoConfiguration(exclude = ...)} 를 달면 그 제외가 다른 테스트의
     * 컨텍스트까지 따라간다 — 같은 실수로 {@code ApiApplicationTests} 가 한 번 깨졌다.
     */
    private static final String EXCLUDE_DATASOURCE =
            "spring.autoconfigure.exclude="
                    + "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration,"
                    + "org.springframework.boot.hibernate.autoconfigure."
                    + "HibernateJpaAutoConfiguration";

    @TempDir
    static Path configDir;

    /** {@code .example} 확장자로는 Spring 이 yaml 로 읽지 않아 이름을 바꿔 복사한다. */
    private static String stagedLocation(String resource, String asName) throws IOException {
        Path target = configDir.resolve(asName);
        try (InputStream source = new ClassPathResource(resource).getInputStream()) {
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return "file:" + target.toAbsolutePath();
    }

    /**
     * 일부러 잘못 쓴 설정. {@code observation.yml.example} 과 값은 같고 <b>키 표기만</b> 점
     * 표기다 — 바로 위 대괄호 표기와 견줘 보라고 여기 인라인해 뒀다.
     */
    private static final String DOT_NOTATION_YAML = """
            management:
              metrics:
                distribution:
                  percentiles:
                    http.server.requests: 0.5, 0.95, 0.99
                  expiry:
                    http.server.requests: 10s
            """;

    private static String stagedYaml(String yaml, String asName) throws IOException {
        Path target = configDir.resolve(asName);
        Files.writeString(target, yaml);
        return "file:" + target.toAbsolutePath();
    }

    private ConfigurableApplicationContext boot(String location) {
        return new SpringApplicationBuilder(TestApp.class)
                .web(WebApplicationType.NONE)
                .bannerMode(org.springframework.boot.Banner.Mode.OFF)
                .properties("spring.config.location=" + location, EXCLUDE_DATASOURCE)
                .run();
    }

    /** 운영에서 로드되는 것과 같은 내용 — 커밋되는 .example 을 그대로 복사한 것이다. */
    private ConfigurableApplicationContext bootWithObservationYaml() throws IOException {
        return boot(stagedLocation("observation.yml.example", "observation.yml"));
    }

    private Timer loadedTimer(ConfigurableApplicationContext context) {
        Timer timer = Timer.builder(MeterNames.HTTP_SERVER_REQUESTS)
                .register(context.getBean(MeterRegistry.class));
        for (int i = 0; i < 200; i++) {
            timer.record(Duration.ofMillis(500));
        }
        return timer;
    }

    @Test
    @DisplayName("observation.yml 의 대괄호 키가 MeterNames 상수의 미터에 0.99 를 실제로 건다")
    void bracketNotationBindsPercentilesToTheNamedMeter() throws IOException {
        try (ConfigurableApplicationContext context = bootWithObservationYaml()) {
            loadedTimer(context);

            MeterValueReader reader = new MeterValueReader(context.getBean(MeterRegistry.class));
            assertThat(reader.percentileNanos(MeterNames.HTTP_SERVER_REQUESTS, P99))
                    .isPresent();
            assertThat(reader.percentileNanos(MeterNames.HTTP_SERVER_REQUESTS, P99).getAsDouble())
                    .isGreaterThan(0);
        }
    }

    @Test
    @DisplayName("OBS-4 latency Timer도 커밋된 설정에서 p99와 10초 expiry를 함께 받는다")
    void customHttpLatencyUsesCommittedDistributionContract() throws IOException {
        try (ConfigurableApplicationContext context = bootWithObservationYaml()) {
            Timer timer = Timer.builder(MeterNames.HTTP_LATENCY)
                    .tags("uri_group", "issue", "outcome", "success")
                    .register(context.getBean(MeterRegistry.class));
            for (int i = 0; i < 200; i++) {
                timer.record(Duration.ofMillis(300));
            }
            MeterValueReader reader = new MeterValueReader(context.getBean(MeterRegistry.class));
            assertThat(timer.getId().getTag("uri_group")).isEqualTo("issue");
            assertThat(timer.getId().getTag("outcome")).isEqualTo("success");
            assertThat(reader.percentileNanos(MeterNames.HTTP_LATENCY, P99)).isPresent();

            context.getBean(MockClock.class).add(Duration.ofSeconds(11));
            assertThat(reader.percentileNanos(MeterNames.HTTP_LATENCY, P99)).isEmpty();
        }
    }

    /**
     * 점 표기의 실패는 <b>절반만</b> 일어난다(실측). Boot 4.1 에서 {@code percentiles} 는 점
     * 표기로도 바인딩되지만 {@code expiry} 는 되지 않는다. 0.99 가 멀쩡히 나오니 설정이 먹은
     * 줄 알게 되고, 정작 목적인 expiry 단축만 조용히 사라진다 — 순수한 함정보다 나쁘다.
     */
    @Test
    @DisplayName("점 표기는 percentiles 만 먹고 expiry 는 조용히 무시된다 — 절반만 성공해서 더 위험하다")
    void dotNotationBindsPercentilesButSilentlyDropsExpiry() throws IOException {
        try (ConfigurableApplicationContext context =
                     boot(stagedYaml(DOT_NOTATION_YAML, "dot-notation.yml"))) {
            loadedTimer(context);
            MeterValueReader reader = new MeterValueReader(context.getBean(MeterRegistry.class));

            assertThat(reader.percentileNanos(MeterNames.HTTP_SERVER_REQUESTS, P99))
                    .as("0.99 는 나온다 — 그래서 설정이 먹은 줄 안다")
                    .isPresent();

            context.getBean(MockClock.class).add(Duration.ofSeconds(11));
            assertThat(reader.percentileNanos(MeterNames.HTTP_SERVER_REQUESTS, P99))
                    .as("expiry 는 안 먹어서 기본 2분이 그대로다 — 11초 뒤에도 값이 살아 있다")
                    .isPresent();
        }
    }

    @Test
    @DisplayName("부하가 멈추면 10초 안에 p99 가 사라진다 — 기본 2분을 그대로 두면 안 되는 이유")
    void percentileDecaysWithinTenSeconds() throws IOException {
        try (ConfigurableApplicationContext context = bootWithObservationYaml()) {
            loadedTimer(context);
            MockClock clock = context.getBean(MockClock.class);
            MeterValueReader reader = new MeterValueReader(context.getBean(MeterRegistry.class));

            clock.add(Duration.ofSeconds(9));
            assertThat(reader.percentileNanos(MeterNames.HTTP_SERVER_REQUESTS, P99))
                    .as("아직 창 안이다 — 여기서 사라지면 값이 너무 빨리 죽는 것이다")
                    .isPresent();

            clock.add(Duration.ofSeconds(2));
            assertThat(reader.percentileNanos(MeterNames.HTTP_SERVER_REQUESTS, P99))
                    .as("11초 시점 — expiry 를 2분으로 되돌리면 여기서 깨진다")
                    .isEmpty();
        }
    }

    /**
     * 행위 테스트만으로는 이름 계약이 안 잡힌다. {@code PropertiesMeterFilter} 가 <b>접두어
     * 매칭</b>이라 상수를 {@code http.server.requests.xxx} 로 바꿔도 백분위가 그대로 나온다
     * (실측). 그래서 키 문자열 자체를 여기서 맞춰 본다.
     */
    @Test
    @DisplayName("observation.yml 의 키가 MeterNames 상수와 문자 그대로 같다")
    void yamlKeysAreExactlyTheConstant() throws Exception {
        java.util.Properties yaml = parse("observation.yml.example");
        String prefix = "management.metrics.distribution.";

        assertThat(yaml.getProperty(prefix + "percentiles[" + MeterNames.HTTP_SERVER_REQUESTS + "]"))
                .as("백분위 키")
                .isEqualTo("0.5, 0.95, 0.99");
        assertThat(yaml.getProperty(prefix + "expiry[" + MeterNames.HTTP_SERVER_REQUESTS + "]"))
                .as("expiry 키 — 상수를 바꾸면 여기서 걸린다")
                .isEqualTo("10s");
        assertThat(yaml.getProperty(prefix + "percentiles[" + MeterNames.HTTP_LATENCY + "]"))
                .as("OBS-4 백분위 키")
                .isEqualTo("0.5, 0.95, 0.99");
        assertThat(yaml.getProperty(prefix + "expiry[" + MeterNames.HTTP_LATENCY + "]"))
                .as("OBS-4 expiry 키")
                .isEqualTo("10s");
    }

    private java.util.Properties parse(String resource) throws Exception {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(resource));
        yaml.afterPropertiesSet();
        return yaml.getObject();
    }

    /**
     * 위 테스트들은 파일을 직접 지목하므로, import 가 빠져 설정 전체가 죽어도 전부 통과한다.
     * 그 구멍을 여기서 막는다 — 커밋되는 {@code .example} 이 import 를 들고 있는지 본다.
     */
    @Test
    @DisplayName("application.yml.example 이 observation.yml 을 import 한다 — 빠지면 설정이 통째로 죽는다")
    void exampleImportsObservationYaml() throws Exception {
        java.util.Properties properties = parse("application.yml.example");
        java.util.List<Object> imports = properties.stringPropertyNames().stream()
                .filter(key -> key.startsWith("spring.config.import["))
                .map(properties::getProperty)
                .collect(java.util.stream.Collectors.toList());

        assertThat(imports).contains("classpath:observation.yml");
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class TestApp {

        @Bean
        MockClock mockClock() {
            return new MockClock();
        }

        @Bean
        MeterRegistry meterRegistry(MockClock clock) {
            return new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);
        }
    }
}

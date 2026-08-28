package com.kafkick.batch.observation;

import static org.assertj.core.api.Assertions.assertThat;

import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.hasPositivePercentile;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.metricsAutoConfigurations;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.percentiles;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.recordOneCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.data.redis.autoconfigure.ClientResourcesBuilderCustomizer;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import com.kafkick.infra.redis.observation.RedisLatencyAutoConfiguration;
import com.kafkick.testsupport.CommittedConfigStager;

/**
 * 커밋되는 {@code management.yml} 의 접두사 키가 lettuce 미터에 <b>실제로</b> 걸리는지 본다.
 *
 * <p>템플릿의 문자열만 대조하는 테스트는 키 표기가 바뀌어도, 미터 이름이 바뀌어도 통과한다.
 * 그 사이(= {@code PropertiesMeterFilter} 가 접두사 하나로 firstresponse · completion 두 미터를
 * 함께 덮는다)를 통과시키는 것이 이 테스트다 — {@code management.yml.example} 이 단정하는 문장의
 * 유일한 근거다.
 *
 * <p>값을 프로퍼티로 주입하지 않고 .example 을 임시 디렉터리에 복사해 로딩 경로를 태운다.
 * 프로퍼티로 주면 대괄호 표기처럼 이 파일에서만 나는 오류를 못 잡는다. api 의
 * {@code ObservationMetricsContractTest} 와 같은 방식이다.
 */
class RedisLatencyManagementYamlContractTest {

    private static final String FIRST_RESPONSE_METER =
        RedisLatencyAutoConfiguration.COMMAND_METER_NAMESPACE + ".firstresponse";
    private static final String COMPLETION_METER =
        RedisLatencyAutoConfiguration.COMMAND_METER_NAMESPACE + ".completion";

    @TempDir
    static Path configDir;

    private final MockClock clock = new MockClock();

    @Test
    @DisplayName("접두사 키 하나가 firstresponse · completion 두 미터에 백분위를 건다")
    void prefixKeyBindsPercentilesToBothMeters() {
        runner().run(context -> {
            recordOneCommand(context.getBean(ClientResourcesBuilderCustomizer.class));
            MeterRegistry registry = context.getBean(MeterRegistry.class);

            assertThat(percentiles(registry, FIRST_RESPONSE_METER))
                .as("firstresponse 에 백분위가 걸려야 한다")
                .containsExactly(0.5, 0.95, 0.99);
            assertThat(percentiles(registry, COMPLETION_METER))
                .as("접두사 키가 completion 까지 덮지 못하면 이쪽만 조용히 빈다")
                .containsExactly(0.5, 0.95, 0.99);
        });
    }

    @Test
    @DisplayName("관측 창은 10초다 — 지나면 p99 가 사라진다")
    void expiryClosesTheObservationWindow() {
        runner().run(context -> {
            recordOneCommand(context.getBean(ClientResourcesBuilderCustomizer.class));
            MeterRegistry registry = context.getBean(MeterRegistry.class);

            clock.add(Duration.ofSeconds(9));
            assertThat(hasPositivePercentile(registry, FIRST_RESPONSE_METER))
                .as("10초 이전에는 값이 유지된다")
                .isTrue();

            clock.add(Duration.ofSeconds(2));
            assertThat(hasPositivePercentile(registry, FIRST_RESPONSE_METER))
                .as("expiry 가 빠지면 기본 2분이 걸려 여기서 값이 남는다 — HTTP p99 와 창이 어긋난다")
                .isFalse();
        });
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=" + stagedManagementYaml())
            .withConfiguration(AutoConfigurations.of(RedisLatencyAutoConfiguration.class))
            .withConfiguration(AutoConfigurations.of(metricsAutoConfigurations()))
            // 시계만 우리 것으로 바꾼다. MeterRegistryPostProcessor 가 PropertiesMeterFilter 를
            // 모든 MeterRegistry 빈에 적용하므로 운영과 같은 경로를 탄다.
            .withBean(MeterRegistry.class, () -> new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock));
    }

    private static String stagedManagementYaml() {
        Path target = configDir.resolve("management.yml");
        try {
            CommittedConfigStager.stage(target, "management.yml.example");
        } catch (IOException e) {
            throw new IllegalStateException("설정 템플릿을 복사하지 못했다", e);
        }
        return "file:" + target.toAbsolutePath();
    }

}

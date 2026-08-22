package com.kafkick.api.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.EXPIRY_PREFIX;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.HTTP_SERVER_REQUESTS;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.PERCENTILES_PREFIX;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.assertOnRuntimeClasspath;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.normalizeCsv;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.yamlProfile;

import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.hasPositivePercentile;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.metricsAutoConfigurations;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.percentiles;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.recordOneCommand;
import static com.kafkick.infra.redis.observation.RedisLatencyMeterTestSupport.yaml;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

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
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import com.kafkick.infra.redis.observation.RedisLatencyAutoConfiguration;
import com.kafkick.testsupport.CommittedConfigStager;

/**
 * api 도 {@code infra:redis} 를 물고 있어(api/build.gradle) {@code RedisLatencyAutoConfiguration} 가 여기서도
 * 자동설정으로 뜬다. 관측 창은 모듈마다 각자의 설정 파일이 쥐므로, api 쪽 {@code observation.yml}
 * 이 lettuce 미터를 실제로 덮는지 여기서 본다.
 *
 * <p>한쪽만 두면 그 JVM 의 Redis p99 만 기본 2분 창이 되어 HTTP p99(10초 창)와 같은 시점의 값이
 * 아니게 된다 — 부하를 끊은 직후 "HTTP 는 정상인데 Redis 가 느리다" 는 오독이 나온다.
 * batch 쪽 같은 계약은 {@code RedisLatencyManagementYamlContractTest}(batch)가 본다.
 */
class RedisLatencyObservationYamlContractTest {

    private static final String FIRST_RESPONSE_METER =
        RedisLatencyAutoConfiguration.COMMAND_METER_NAMESPACE + ".firstresponse";
    private static final String COMPLETION_METER =
        RedisLatencyAutoConfiguration.COMMAND_METER_NAMESPACE + ".completion";


    @TempDir
    static Path configDir;

    private final MockClock clock = new MockClock();

    @Test
    @DisplayName("계측 모듈이 api 런타임 클래스패스에 실려 있어야 한다")
    void redisLatencyModuleIsOnRuntimeClasspath() {
        // batch 와 대칭이다. 근거는 assertOnRuntimeClasspath 의 javadoc 에 있다.
        assertOnRuntimeClasspath(":infra:redis");
    }

    @Test
    @DisplayName("observation.yml 의 접두사 키가 두 lettuce 미터에 백분위를 건다")
    void prefixKeyBindsPercentilesToBothMeters() {
        runner().run(context -> {
            recordOneCommand(context.getBean(ClientResourcesBuilderCustomizer.class));
            MeterRegistry registry = context.getBean(MeterRegistry.class);

            assertThat(percentiles(registry, FIRST_RESPONSE_METER)).containsExactly(0.5, 0.95, 0.99);
            assertThat(percentiles(registry, COMPLETION_METER))
                .as("접두사 키가 completion 까지 덮지 못하면 이쪽만 조용히 빈다")
                .containsExactly(0.5, 0.95, 0.99);
        });
    }

    @Test
    @DisplayName("관측 창이 HTTP 미터와 같은 10초다")
    void expiryMatchesTheHttpObservationWindow() {
        runner().run(context -> {
            recordOneCommand(context.getBean(ClientResourcesBuilderCustomizer.class));
            MeterRegistry registry = context.getBean(MeterRegistry.class);

            clock.add(Duration.ofSeconds(9));
            assertThat(hasPositivePercentile(registry, FIRST_RESPONSE_METER))
                .as("10초 이전에는 값이 유지된다")
                .isTrue();

            clock.add(Duration.ofSeconds(2));
            assertThat(hasPositivePercentile(registry, FIRST_RESPONSE_METER))
                .as("expiry 가 빠지면 기본 2분이 걸려 여기서 값이 남는다")
                .isFalse();
        });
    }

    @Test
    @DisplayName("관측 창을 HTTP 미터와 같은 값으로 적어 뒀는지 템플릿에서도 고정한다")
    void templateKeepsLettuceExpiryEqualToHttp() {
        // 위 실행 테스트는 10초라는 값 자체를 고정한다. 이 테스트는 그 값이 HTTP 쪽과 같은
        // 값이어야 한다는 계약을 고정한다 — 나중에 HTTP 창만 조이면 여기서 깨진다.
        Properties template = observationTemplate();
        String lettuce = template.getProperty(
            EXPIRY_PREFIX + "[" + RedisLatencyAutoConfiguration.COMMAND_METER_NAMESPACE + "]");
        String http = template.getProperty(EXPIRY_PREFIX + "[" + HTTP_SERVER_REQUESTS + "]");

        assertThat(http).as("HTTP 쪽 키 표기가 바뀌면 비교 자체가 무의미해진다").isNotNull();
        assertThat(lettuce)
            .as("관측 창이 다르면 HTTP p99 와 Redis p99 가 같은 시점의 값이 아니다")
            .isEqualTo(http);
    }

    @Test
    @DisplayName("배포 설정이 관측 창을 덮어쓰면 모듈 값과 같아야 한다")
    void deployedConfigDoesNotDivergeOnDistributionWindows() {
        // 관측 창은 저장소에 사본이 둘이다 — 이 모듈의 observation.yml 과 컨테이너에 마운트되는
        // 루트 application.yml(언급한 키는 그쪽이 이긴다). http 만 대조하면 배포 설정에
        // "[lettuce.command]": 120s 를 얹는 변경이 통째로 안 잡힌다(실측).
        //
        // 배포 설정이 언급하지 않은 키는 모듈 값이 그대로 살아남으므로 없는 것은 통과시킨다.
        Properties deployed = yamlProfile(deployedResource(), "api");
        Properties module = observationTemplate();

        for (String prefix : List.of(EXPIRY_PREFIX, PERCENTILES_PREFIX)) {
            for (String meter : List.of(HTTP_SERVER_REQUESTS,
                    RedisLatencyAutoConfiguration.COMMAND_METER_NAMESPACE)) {
                String key = prefix + "[" + meter + "]";
                String deployedValue = deployed.getProperty(key);
                if (deployedValue == null) {
                    continue;
                }
                assertThat(normalizeCsv(deployedValue))
                    .as("배포 설정이 " + key + " 를 덮으면 모듈 값과 같아야 한다 — 다르면 두 p99 가"
                        + " 같은 시점의 값이 아니다")
                    .isEqualTo(normalizeCsv(module.getProperty(key)));
            }
        }
    }


    private static FileSystemResource deployedResource() {
        return new FileSystemResource(Path.of("..", "application.yml.example"));
    }

    private static Properties observationTemplate() {
        return yaml(new ClassPathResource("observation.yml.example"));
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=" + stagedObservationYaml())
            .withConfiguration(AutoConfigurations.of(RedisLatencyAutoConfiguration.class))
            .withConfiguration(AutoConfigurations.of(metricsAutoConfigurations()))
            .withBean(MeterRegistry.class, () -> new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock));
    }

    private static String stagedObservationYaml() {
        Path target = configDir.resolve("observation.yml");
        try {
            CommittedConfigStager.stage(target, "observation.yml.example");
        } catch (IOException e) {
            throw new IllegalStateException("설정 템플릿을 복사하지 못했다", e);
        }
        return "file:" + target.toAbsolutePath();
    }

}

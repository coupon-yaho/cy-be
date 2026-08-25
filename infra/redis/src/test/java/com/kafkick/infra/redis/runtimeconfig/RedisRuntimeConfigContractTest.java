package com.kafkick.infra.redis.runtimeconfig;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.ReadOnlyRuntimeConfigStore;
import com.kafkick.core.runtimeconfig.RuntimeConfigCommand;
import com.kafkick.core.runtimeconfig.RuntimeConfigErrorCode;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;

import static org.assertj.core.api.Assertions.assertThat;

class RedisRuntimeConfigContractTest {

    private static final Path REPO_ROOT = Path.of("../..").toAbsolutePath().normalize();

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RuntimeConfigRedisAutoConfiguration.class))
            .withBean(StringRedisTemplate.class, () -> org.mockito.Mockito.mock(StringRedisTemplate.class));

    /** Redis 자동설정이 Runtime Config Store를 제공하는지 검증합니다. */
    @Test
    void autoConfigurationProvidesRuntimeConfigStore() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(RuntimeConfigStore.class);
        });
    }

    /** 사용자가 제공한 Store가 있으면 Redis 자동설정이 물러나는지 검증합니다. */
    @Test
    void autoConfigurationBacksOffForCustomRuntimeConfigStore() {
        RuntimeConfigStore customStore = new ReadOnlyRuntimeConfigStore(new RuntimeConfigSnapshot(
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                3L,
                Instant.parse("2026-08-26T00:00:00Z"),
                "812934",
                SourceStatus.VALID));

        contextRunner.withBean(RuntimeConfigStore.class, () -> customStore)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(RuntimeConfigStore.class);
                    assertThat(context.getBean(RuntimeConfigStore.class)).isSameAs(customStore);
                });
    }

    @Test
    void redisTimeoutsAreBoundedAndEnvironmentOverrideable() throws Exception {
        String config = Files.readString(Path.of("src/main/resources/redis.yml.example"));

        assertThat(config).contains(
                "host: ${REDIS_HOST:localhost}",
                "port: ${REDIS_PORT:6379}",
                "connect-timeout: ${REDIS_CONNECT_TIMEOUT:1s}",
                "timeout: ${REDIS_COMMAND_TIMEOUT:500ms}");
        assertThat(config).doesNotContain("max-wait");
    }

    @Test
    void runtimeStoreDoesNotDependOnAutoConfigurationEvaluationOrder() throws Exception {
        Method factory = RuntimeConfigRedisAutoConfiguration.class.getDeclaredMethod(
                "runtimeConfigStore",
                org.springframework.data.redis.core.StringRedisTemplate.class,
                RuntimeConfigJsonMapper.class,
                org.springframework.beans.factory.ObjectProvider.class);

        assertThat(factory.getAnnotation(ConditionalOnBean.class)).isNull();
        assertThat(factory.getReturnType()).isEqualTo(RuntimeConfigStore.class);
    }

    @Test
    void runtimeConfigPersistenceUsesItsOwnFixedObjectMapper() throws Exception {
        Method mapperFactory = RuntimeConfigRedisAutoConfiguration.class.getDeclaredMethod(
                "runtimeConfigJsonMapper");
        RuntimeConfigJsonMapper jsonMapper = (RuntimeConfigJsonMapper) mapperFactory.invoke(
                new RuntimeConfigRedisAutoConfiguration());
        ObjectMapper mapper = jsonMapper.objectMapper();
        String json = mapper.writeValueAsString(new RuntimeConfigCommand(
                com.kafkick.core.observation.EngineVersion.V2,
                com.kafkick.core.observation.ReleaseStage.V2_2,
                com.kafkick.core.observation.QueueMode.OFF,
                "admin:1"));
        Method storeFactory = RuntimeConfigRedisAutoConfiguration.class.getDeclaredMethod(
                "runtimeConfigStore",
                org.springframework.data.redis.core.StringRedisTemplate.class,
                RuntimeConfigJsonMapper.class,
                org.springframework.beans.factory.ObjectProvider.class);

        assertThat(json).contains("\"engineVersion\"", "\"releaseStage\"")
                .doesNotContain("engine_version", "release_stage");
        assertThat(storeFactory.getParameterTypes()[1]).isEqualTo(RuntimeConfigJsonMapper.class);
    }

    @Test
    void newEnvironmentSetupRequiresExplicitIdempotentRuntimeConfigSeed() throws Exception {
        String readme = Files.readString(REPO_ROOT.resolve("README.md"));
        String compose = Files.readString(REPO_ROOT.resolve("compose.yml"));

        assertThat(readme).contains(
                "docker compose up -d redis",
                "docker compose --profile runtime-config-seed run --rm runtime-config-seed");
        assertThat(compose).contains(
                "profiles: [\"runtime-config-seed\"]",
                "redis-cli -h redis SET config:runtime",
                "NX");
    }

    @Test
    void ignoredRedisConfigHasARequiredExampleCopyPath() throws Exception {
        Path example = Path.of("src/main/resources/redis.yml.example");
        String application = Files.readString(
                REPO_ROOT.resolve("api/src/main/resources/application.yml.example"));
        String readme = Files.readString(REPO_ROOT.resolve("README.md"));
        String gitignore = Files.readString(REPO_ROOT.resolve(".gitignore"));

        assertThat(example).exists();
        assertThat(application).contains("- classpath:redis.yml");
        assertThat(readme).contains(
                "`application.yml`, `storage.yml`, `redis.yml`",
                "*/src/main/resources/*.yml.example",
                "${1%.example}");
        assertThat(gitignore).contains("**/src/main/resources/redis.yml");
    }

    @Test
    void repositoryCiMustExecuteTheActualRedisLuaTests() throws Exception {
        Path workflow = REPO_ROOT.resolve(".github/workflows/test.yml");

        assertThat(workflow).exists();
        assertThat(Files.readString(workflow)).contains(
                "docker version",
                "RedisRuntimeConfigIntegrationTest",
                "./gradlew test");
    }

    @Test
    void storeUnavailableIsAttributedToRedis() {
        assertThat(RuntimeConfigErrorCode.STORE_UNAVAILABLE.dependency())
                .isEqualTo(Dependency.REDIS);
        assertThat(RuntimeConfigErrorCode.STORE_UNAVAILABLE.reasonCode())
                .contains(ReasonCode.TEMPORARILY_UNAVAILABLE);
    }
}

package com.kafkick.infra.redis.runtimeconfig;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

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
            assertThat(context).hasSingleBean(RuntimeConfigBootstrap.class);
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
                    assertThat(context).doesNotHaveBean(RuntimeConfigBootstrap.class);
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
    void redisConfigurationKeepsStandaloneDefaultAndEnablesSentinelOnlyWhenProvided() throws Exception {
        String config = Files.readString(Path.of("src/main/resources/redis.yml.example"));

        // Sentinel 을 켜는 것은 프로파일이지 값의 유무가 아니다(파일 주석 참고).
        // 그래서 두 키에는 **빈 기본값**을 둔다 — 없으면 환경변수 미정의 시 프로퍼티 해석이
        // 먼저 터져 "Could not resolve placeholder" 가 나가고, 무엇이 필요한지 말해 주는
        // RedisSentinelConfigurationGuardAutoConfiguration 의 메시지가 영영 안 나온다.
        assertThat(config).contains(
                "host: ${REDIS_HOST:localhost}",
                "port: ${REDIS_PORT:6379}",
                "on-profile: redis-sentinel",
                "master: ${REDIS_SENTINEL_MASTER:}",
                "nodes: ${REDIS_SENTINEL_NODES:}");
    }

    @Test
    void redisConfigurationDeclaresTheSharedRuntimeConfigBootstrapDefaults() throws Exception {
        String config = Files.readString(Path.of("src/main/resources/redis.yml.example"));

        assertThat(config).contains(
                "runtime-config:",
                "bootstrap:",
                "engine-version: ${RUNTIME_CONFIG_BOOTSTRAP_ENGINE_VERSION:V1}",
                "release-stage: ${RUNTIME_CONFIG_BOOTSTRAP_RELEASE_STAGE:V1}",
                "queue-mode: ${RUNTIME_CONFIG_BOOTSTRAP_QUEUE_MODE:OFF}");
    }

    @Test
    @SuppressWarnings("unchecked")
    void runtimeConfigBootstrapIsInAnUnprofiledYamlDocument() throws Exception {
        try (InputStream stream = Files.newInputStream(Path.of("src/main/resources/redis.yml.example"))) {
            Iterator<Object> documents = new Yaml().loadAll(stream).iterator();
            Map<String, Object> standalone = (Map<String, Object>) documents.next();
            Map<String, Object> sentinel = (Map<String, Object>) documents.next();
            Map<String, Object> runtime = (Map<String, Object>) documents.next();

            assertThat(standalone).doesNotContainKey("runtime-config");
            assertThat(((Map<String, Object>) sentinel.get("spring")).get("config")).isNotNull();
            assertThat(runtime).containsKey("runtime-config");
        }
    }

    @Test
    void runtimeStoreDoesNotDependOnAutoConfigurationEvaluationOrder() throws Exception {
        Method factory = RuntimeConfigRedisAutoConfiguration.class.getDeclaredMethod(
                "runtimeConfigStore",
                org.springframework.data.redis.core.StringRedisTemplate.class,
                RuntimeConfigJsonMapper.class,
                org.springframework.beans.factory.ObjectProvider.class);

        assertThat(factory.getAnnotation(ConditionalOnBean.class)).isNull();
        assertThat(factory.getReturnType()).isEqualTo(RedisRuntimeConfigStore.class);
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
    void newEnvironmentSetupDocumentsApiBootstrapAndKeepsTheOptionalIdempotentSeed() throws Exception {
        String readme = Files.readString(REPO_ROOT.resolve("README.md"));
        String compose = Files.readString(REPO_ROOT.resolve("compose.yml"));

        assertThat(readme).contains(
                "`config:runtime`은 API 기동 시에만 부트스트랩한다.",
                "별도 시드 없이 시작할 수 있다.",
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

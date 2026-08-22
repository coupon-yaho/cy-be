package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.runtimeconfig.RuntimeConfigErrorCode;
import com.kafkick.core.runtimeconfig.RuntimeConfigCommand;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import tools.jackson.databind.ObjectMapper;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class RedisRuntimeConfigContractTest {

    private static final Path REPO_ROOT = Path.of("../..").toAbsolutePath().normalize();

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

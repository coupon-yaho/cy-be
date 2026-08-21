package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.runtimeconfig.RuntimeConfigCommand;
import com.kafkick.core.runtimeconfig.RuntimeConfigRevisionConflictException;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class RedisRuntimeConfigIntegrationTest {

    @Container
    private static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);
    private static final Instant NOW = Instant.parse("2026-08-20T10:00:00Z");

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redis;
    private static ObjectMapper objectMapper;

    @BeforeAll
    static void startRedis() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        redis = new StringRedisTemplate(connectionFactory);
        redis.afterPropertiesSet();
        objectMapper = new RuntimeConfigJsonMapper().objectMapper();
    }

    @AfterAll
    static void stopRedis() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @BeforeEach
    void clearRedis() {
        try (RedisConnection connection = connectionFactory.getConnection()) {
            connection.serverCommands().flushDb();
        }
    }

    @Test
    void actualLuaAllowsOneConcurrentWinnerAndAppendsOneAuditRecord() throws Exception {
        seed(snapshot(0));
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));
        int contenders = 12;
        CountDownLatch ready = new CountDownLatch(contenders);
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(contenders)) {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int index = 0; index < contenders; index++) {
                String updater = "admin:" + index;
                results.add(executor.submit(() -> {
                    ready.countDown();
                    start.await();
                    try {
                        store.update(new RuntimeConfigCommand(
                                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, updater), 0);
                        return true;
                    } catch (RuntimeConfigRevisionConflictException exception) {
                        return false;
                    }
                }));
            }
            ready.await();
            start.countDown();

            long successes = 0;
            for (Future<Boolean> result : results) {
                if (result.get()) {
                    successes++;
                }
            }
            assertThat(successes).isEqualTo(1);
        }

        RuntimeConfigSnapshot stored = store.get();
        assertThat(stored.revision()).isEqualTo(1);
        List<String> audits = redis.opsForList().range("config:runtime:audit", 0, -1);
        assertThat(audits).singleElement().satisfies(json -> {
            RuntimeConfigAuditLog audit = objectMapper.readValue(json, RuntimeConfigAuditLog.class);
            assertThat(audit.previousRevision()).isZero();
            assertThat(audit.newRevision()).isEqualTo(1);
            assertThat(audit.beforeConfig()).isEqualTo(snapshot(0));
            assertThat(audit.afterConfig()).isEqualTo(stored);
        });
    }

    @Test
    void actualLuaRecordsTheStoredConfigurationAsAuditBefore() throws Exception {
        seed(snapshot(0));
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));
        RuntimeConfigSnapshot before = store.update(new RuntimeConfigCommand(
                EngineVersion.V2, ReleaseStage.V2_2, QueueMode.OFF, "admin:1"), 0);

        store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:2"), 1);

        List<String> audits = redis.opsForList().range("config:runtime:audit", 0, -1);
        assertThat(audits).hasSize(2);
        RuntimeConfigAuditLog second = objectMapper.readValue(audits.get(1), RuntimeConfigAuditLog.class);
        assertThat(second.previousRevision()).isEqualTo(1);
        assertThat(second.beforeConfig()).isEqualTo(before);
        assertThat(second.afterConfig().revision()).isEqualTo(2);
    }

    @Test
    void auditKeepsOnlyTheLatestOneThousandSuccessfulChanges() throws Exception {
        seed(snapshot(0));
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        for (long revision = 0; revision <= 1_000; revision++) {
            store.update(new RuntimeConfigCommand(
                    EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), revision);
        }

        List<String> audits = redis.opsForList().range("config:runtime:audit", 0, -1);
        assertThat(audits).hasSize(1_000);
        assertThat(objectMapper.readValue(audits.getFirst(), RuntimeConfigAuditLog.class).previousRevision())
                .isEqualTo(1);
        assertThat(objectMapper.readValue(audits.getLast(), RuntimeConfigAuditLog.class).previousRevision())
                .isEqualTo(1_000);
    }

    @Test
    void missingKeyWithNonzeroRevisionIsDataLossNotConflict() {
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 500))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-007");
    }

    @Test
    void missingKeyWithZeroRevisionCannotImplicitlySeedTheStore() {
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 0))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-007");
    }

    @Test
    void corruptedJsonCannotBeOverwrittenByOrdinaryUpdate() {
        redis.opsForValue().set("config:runtime", "{not-json");
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 1))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-006");
    }

    @Test
    void negativeStoredRevisionIsCorruptionNotCasSuccess() {
        redis.opsForValue().set("config:runtime", """
                {"engineVersion":"V2","releaseStage":"V2_2","queueMode":"OFF",\
                "revision":-1,"updatedAt":"2026-08-20T09:59:00Z",\
                "updatedBy":"seed","status":"VALID"}
                """);
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 0))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-006");
    }

    @Test
    void fractionalStoredRevisionIsCorruptionNotRevisionConflict() {
        redis.opsForValue().set("config:runtime", """
                {"engineVersion":"V2","releaseStage":"V2_2","queueMode":"OFF",\
                "revision":3.5,"updatedAt":"2026-08-20T09:59:00Z",\
                "updatedBy":"seed","status":"VALID"}
                """);
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 3))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode())
                .isEqualTo(com.kafkick.core.runtimeconfig.RuntimeConfigErrorCode.STORE_CORRUPTED);
    }

    @Test
    void scalarJsonIsCorruptionNotRedisOutage() {
        redis.opsForValue().set("config:runtime", "5");
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 0))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-006");
    }

    @Test
    void wrongAuditKeyTypeCannotPartiallyApplyConfiguration() {
        RuntimeConfigSnapshot before = snapshot(0);
        seed(before);
        redis.opsForValue().set("config:runtime:audit", "wrong-type");
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 0))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-006");
        assertThat(store.get()).isEqualTo(before);
    }

    @Test
    void wrongConfigKeyTypeIsCorruptionNotRedisOutage() {
        redis.opsForList().leftPush("config:runtime", "wrong-type");
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1"), 0))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-006");
    }

    private static RuntimeConfigSnapshot snapshot(long revision) {
        return new RuntimeConfigSnapshot(
                EngineVersion.V2, ReleaseStage.V2_2, QueueMode.OFF, revision,
                NOW.minusSeconds(60), "seed", com.kafkick.core.observation.SourceStatus.VALID);
    }

    private static void seed(RuntimeConfigSnapshot snapshot) {
        redis.opsForValue().set("config:runtime", objectMapper.writeValueAsString(snapshot));
    }
}

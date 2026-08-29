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
    void bootstrapSeedsAnEmptyRedisWithRevisionZero() throws Exception {
        RuntimeConfigBootstrapProperties properties = bootstrapProperties(
                EngineVersion.V2, ReleaseStage.V2_1, QueueMode.ALWAYS);

        bootstrap(properties).run(new org.springframework.boot.DefaultApplicationArguments());

        assertThat(new RedisRuntimeConfigStore(redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC)).get())
                .isEqualTo(new RuntimeConfigSnapshot(
                        EngineVersion.V2, ReleaseStage.V2_1, QueueMode.ALWAYS,
                        0, NOW, "system:bootstrap", com.kafkick.core.observation.SourceStatus.VALID));
    }

    @Test
    void bootstrapDoesNotOverwriteAnExistingRevision() throws Exception {
        RuntimeConfigSnapshot existing = snapshot(9);
        seed(existing);

        bootstrap(bootstrapProperties(EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE))
                .run(new org.springframework.boot.DefaultApplicationArguments());

        assertThat(new RedisRuntimeConfigStore(redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC)).get())
                .isEqualTo(existing);
    }

    @Test
    void concurrentBootstrapsHaveOneSetNxWinner() throws Exception {
        RuntimeConfigBootstrap first = bootstrap(bootstrapProperties(
                EngineVersion.V2, ReleaseStage.V2_1, QueueMode.ALWAYS));
        RuntimeConfigBootstrap second = bootstrap(bootstrapProperties(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE));
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);

        try (var executor = Executors.newFixedThreadPool(2)) {
            List<Future<?>> results = List.of(
                    executor.submit(() -> runBootstrap(first, ready, start)),
                    executor.submit(() -> runBootstrap(second, ready, start)));
            ready.await();
            start.countDown();
            for (Future<?> result : results) {
                result.get();
            }
        }

        RuntimeConfigSnapshot stored = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC)).get();
        assertThat(stored.revision()).isZero();
        assertThat(stored.updatedBy()).isEqualTo("system:bootstrap");
        assertThat(stored.engineVersion()).isIn(EngineVersion.V2, EngineVersion.V3);
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

    private RuntimeConfigBootstrap bootstrap(RuntimeConfigBootstrapProperties properties) {
        return new RuntimeConfigBootstrap(redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC), properties);
    }

    private static RuntimeConfigBootstrapProperties bootstrapProperties(
            EngineVersion engineVersion, ReleaseStage releaseStage, QueueMode queueMode
    ) {
        RuntimeConfigBootstrapProperties properties = new RuntimeConfigBootstrapProperties();
        properties.setEngineVersion(engineVersion);
        properties.setReleaseStage(releaseStage);
        properties.setQueueMode(queueMode);
        return properties;
    }

    private static void runBootstrap(
            RuntimeConfigBootstrap bootstrap, CountDownLatch ready, CountDownLatch start
    ) {
        try {
            ready.countDown();
            start.await();
            bootstrap.run(new org.springframework.boot.DefaultApplicationArguments());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError(exception);
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    // 시드 JSON 이 스냅샷 포맷과 어긋나면 update 가 아니라 첫 조회에서 드러나야 한다.
    // 신규 환경은 기동 직후 읽기부터 하므로, 이 경로가 조용하면 잘못된 시드가 잠복한다.
    @Test
    void seedThatDoesNotMatchTheSnapshotFormatSurfacesOnTheFirstRead() {
        redis.opsForValue().set("config:runtime", "{\"engineVersion\":\"V1\",\"revision\":0}");
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(store::get)
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-006");
    }

    @Test
    void seedWrittenByComposeIsReadableAsASnapshotWithRevisionZero() {
        redis.opsForValue().set("config:runtime", composeSeedJson());
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        RuntimeConfigSnapshot seeded = store.get();

        assertThat(seeded.revision()).isZero();
        assertThat(seeded.status()).isEqualTo(com.kafkick.core.observation.SourceStatus.VALID);
        assertThat(seeded.queueMode()).isEqualTo(QueueMode.OFF);
    }

    // compose.yml 의 시드 명령에서 JSON 을 그대로 읽어온다. 문자열을 여기에 다시 적으면
    // compose 와 테스트가 각자 흘러가 드리프트를 못 잡는다.
    private static String composeSeedJson() {
        String compose;
        try {
            compose = java.nio.file.Files.readString(
                    java.nio.file.Path.of("../..").toAbsolutePath().normalize().resolve("compose.yml"));
        } catch (java.io.IOException exception) {
            throw new java.io.UncheckedIOException(exception);
        }
        int begin = compose.indexOf("{\\\"engineVersion");
        assertThat(begin).as("compose.yml 의 시드 JSON").isNotNegative();
        int end = compose.indexOf("\" NX", begin);
        assertThat(end).as("compose.yml 시드 JSON 의 끝").isGreaterThan(begin);
        return compose.substring(begin, end)
                .replace("\\\"", "\"")
                .replace("$${seeded_at}", NOW.toString());
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

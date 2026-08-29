package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigCommand;
import com.kafkick.core.runtimeconfig.RuntimeConfigErrorCode;
import com.kafkick.core.runtimeconfig.RuntimeConfigRevisionConflictException;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.ValueOperations;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class RedisRuntimeConfigStoreTest {

    private static final Instant NOW = Instant.parse("2026-08-20T10:00:00Z");
    private final ObjectMapper objectMapper = new RuntimeConfigJsonMapper().objectMapper();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    void sameExpectedRevisionCanOnlyWinOnceAndAuditHasAllSevenFields() throws Exception {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        RedisRuntimeConfigStore first = store(fake.template);
        RedisRuntimeConfigStore second = store(fake.template);
        MDC.put("requestId", "req-263");

        RuntimeConfigSnapshot updated = first.update(command("admin:1"), 4);

        assertThat(updated.revision()).isEqualTo(5);
        assertThatThrownBy(() -> second.update(command("admin:2"), 4))
                .isInstanceOfSatisfying(RuntimeConfigRevisionConflictException.class,
                        exception -> assertThat(exception.getCurrentRevision()).isEqualTo(5));
        assertThat(fake.audits).hasSize(1);
        RuntimeConfigAuditLog audit = objectMapper.readValue(fake.audits.getFirst(), RuntimeConfigAuditLog.class);
        assertThat(audit.previousRevision()).isEqualTo(4);
        assertThat(audit.newRevision()).isEqualTo(5);
        assertThat(audit.beforeConfig()).isEqualTo(snapshot(4));
        assertThat(audit.afterConfig()).isEqualTo(updated);
        assertThat(audit.changedBy()).isEqualTo("admin:1");
        assertThat(audit.changedAt()).isEqualTo(NOW);
        assertThat(audit.requestId()).isEqualTo("req-263");
    }

    @Test
    void readFallsBackToStaleLkgButWriteOutcomeIsUnknownWhenRedisIsDown() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        RedisRuntimeConfigStore store = store(fake.template);
        assertThat(store.get().status()).isEqualTo(SourceStatus.VALID);
        fake.down = true;

        assertThat(store.get().status()).isEqualTo(SourceStatus.STALE);
        assertThat(store.getLastKnownGood()).contains(snapshot(4));
        assertThatThrownBy(() -> store.update(command("admin:1"), 4))
                .isInstanceOf(BusinessException.class)
                .extracting("errorCode")
                .isEqualTo(RuntimeConfigErrorCode.UPDATE_OUTCOME_UNKNOWN);
    }

    @Test
    void concurrentRedisUpdatesHaveOneWinnerAndOneAuditEntry() throws Exception {
        int contenders = 16;
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
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
                        store(fake.template).update(command(updater), 4);
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
            assertThat(fake.audits).hasSize(1);
            assertThat(objectMapper.readValue(fake.config.get(), RuntimeConfigSnapshot.class).revision())
                    .isEqualTo(5);
        }
    }

    @Test
    void auditUsesTheConfigurationActuallyReplacedByLua() throws Exception {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(3)));
        fake.beforeExecute = () -> fake.config.set(json(snapshot(4)));

        store(fake.template).update(command("admin:1"), 4);

        RuntimeConfigAuditLog audit = objectMapper.readValue(fake.audits.getFirst(), RuntimeConfigAuditLog.class);
        assertThat(audit.previousRevision()).isEqualTo(4);
        assertThat(audit.beforeConfig()).isEqualTo(snapshot(4));
    }

    @Test
    void corruptedJsonIsNotHiddenByTheLastKnownGoodValue() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        RedisRuntimeConfigStore store = store(fake.template);
        store.get();
        fake.config.set("{not-json");

        assertThatThrownBy(store::get)
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-006");
    }

    @Test
    void missingKeyWithoutLastKnownGoodIsNotReportedAsRedisOutage() {
        FakeRedis fake = new FakeRedis(objectMapper, null);

        assertThatThrownBy(() -> store(fake.template).get())
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode())
                .isEqualTo(RuntimeConfigErrorCode.STORE_MISSING);
    }

    @Test
    void keyDeletedAfterAHealthyReadRemainsStaleInsteadOfBeingBootstrappedAgain() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        RedisRuntimeConfigStore store = store(fake.template);
        assertThat(store.get().status()).isEqualTo(SourceStatus.VALID);
        fake.config.set(null);

        RuntimeConfigSnapshot result = store.get();

        assertThat(result.status()).isEqualTo(SourceStatus.STALE);
        assertThat(result.revision()).isEqualTo(4);
        assertThat(fake.config).hasValue(null);
    }

    @Test
    void lastKnownGoodRevisionNeverMovesBackward() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(5)));
        RedisRuntimeConfigStore store = store(fake.template);
        fake.beforeGetReturn = () -> {
            fake.beforeGetReturn = () -> { };
            store.update(command("admin:1"), 5);
        };

        assertThat(store.get().revision()).isEqualTo(5);
        assertThat(store.getLastKnownGood()).get().extracting(RuntimeConfigSnapshot::revision)
                .isEqualTo(6L);
    }

    @Test
    void redisRemainsAuthoritativeWhenItsRevisionIsLowerThanTheLocalLkg() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(9)));
        RedisRuntimeConfigStore store = store(fake.template);
        assertThat(store.get().revision()).isEqualTo(9);
        fake.config.set(json(snapshot(3)));

        assertThat(store.get()).isEqualTo(snapshot(3));
        assertThat(store.getLastKnownGood()).contains(snapshot(3));
    }

    @Test
    void aLaterReadConvergesToTheCurrentRedisRevision() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(9)));
        RedisRuntimeConfigStore store = store(fake.template);
        store.get();
        fake.config.set(json(snapshot(3)));
        fake.beforeGetReturn = () -> {
            fake.beforeGetReturn = () -> { };
            fake.config.set(json(snapshot(10)));
        };

        assertThat(store.get().revision()).isEqualTo(3);
        assertThat(store.get().revision()).isEqualTo(10);
        assertThat(store.getLastKnownGood()).contains(snapshot(10));
    }

    @Test
    void timeoutAfterCommitIsConfirmedByReadingStoredValue() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        fake.timeoutAfterCommit = true;
        RedisRuntimeConfigStore store = store(fake.template);
        MDC.put("requestId", "req-timeout");

        RuntimeConfigSnapshot updated = store.update(command("admin:1"), 4);

        assertThat(updated.revision()).isEqualTo(5);
        assertThat(store.getLastKnownGood()).contains(updated);
    }

    @Test
    void timeoutWithFailedConfirmationReportsUnknownOutcome() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        fake.timeoutAfterCommit = true;
        fake.failReadsAfterUncertainOutcome = true;

        assertThatThrownBy(() -> store(fake.template).update(command("admin:1"), 4))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("RUNTIME_CONFIG-008");
    }

    @Test
    void connectionLossAfterCommitIsConfirmedByReadingStoredValue() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        fake.connectionFailureAfterCommit = true;
        RedisRuntimeConfigStore store = store(fake.template);
        MDC.put("requestId", "req-connection");

        RuntimeConfigSnapshot updated = store.update(command("admin:1"), 4);

        assertThat(updated.revision()).isEqualTo(5);
        assertThat(store.getLastKnownGood()).contains(updated);
    }

    @Test
    void redisSystemExceptionAfterCommitIsConfirmedByAuditRequestId() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        fake.systemFailureAfterCommit = true;
        RedisRuntimeConfigStore store = store(fake.template);
        MDC.put("requestId", "req-system-failure");

        RuntimeConfigSnapshot updated = store.update(command("admin:1"), 4);

        assertThat(updated.revision()).isEqualTo(5);
        assertThat(store.getLastKnownGood()).contains(updated);
    }

    @Test
    void invalidRedisApiUsageIsNotTreatedAsAnUncertainCommit() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        fake.invalidApiUsageBeforeCommit = true;
        MDC.put("requestId", "req-invalid-api");

        assertThatThrownBy(() -> store(fake.template).update(command("admin:1"), 4))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode())
                .isEqualTo(RuntimeConfigErrorCode.STORE_UNAVAILABLE);
    }

    @Test
    void uncertainUpdateIsNotConfirmedByAnotherRequestsIdenticalValue() throws Exception {
        RuntimeConfigSnapshot identical = new RuntimeConfigSnapshot(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, 5,
                NOW, "admin:1", SourceStatus.VALID);
        FakeRedis fake = new FakeRedis(objectMapper, json(identical));
        fake.audits.add(json(new RuntimeConfigAuditLog(
                4, 5, snapshot(4), identical, "admin:1", NOW, "req-other")));
        fake.connectionFailureBeforeCommit = true;
        MDC.put("requestId", "req-mine");

        assertThatThrownBy(() -> store(fake.template).update(command("admin:1"), 4))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode())
                .isEqualTo(RuntimeConfigErrorCode.UPDATE_OUTCOME_UNKNOWN);
    }

    @Test
    void nonUniqueSystemFallbackCannotConfirmAnUncertainUpdate() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        fake.timeoutAfterCommit = true;

        assertThatThrownBy(() -> store(fake.template).update(command("admin:1"), 4))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode())
                .isEqualTo(RuntimeConfigErrorCode.UPDATE_OUTCOME_UNKNOWN);
    }

    @Test
    void confirmationDoesNotReplaceAConcurrentNewerLkg() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        RedisRuntimeConfigStore store = store(fake.template);
        store.get();
        fake.timeoutAfterCommit = true;
        fake.beforeAuditRange = () -> {
            fake.config.set(json(snapshot(6)));
            store.get();
        };
        MDC.put("requestId", "req-mine");

        assertThat(store.update(command("admin:1"), 4).revision()).isEqualTo(5);
        assertThat(store.getLastKnownGood()).contains(snapshot(6));
    }

    @Test
    void connectionLossWithFailedConfirmationReportsUnknownOutcome() {
        FakeRedis fake = new FakeRedis(objectMapper, json(snapshot(4)));
        fake.connectionFailureAfterCommit = true;
        fake.failReadsAfterUncertainOutcome = true;

        assertThatThrownBy(() -> store(fake.template).update(command("admin:1"), 4))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode())
                .isEqualTo(RuntimeConfigErrorCode.UPDATE_OUTCOME_UNKNOWN);
    }

    @Test
    void localSerializationFailureIsNotAttributedToRedisCorruption() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        when(failingMapper.writeValueAsString(any()))
                .thenThrow(mock(tools.jackson.core.JacksonException.class));
        RedisRuntimeConfigStore store = new RedisRuntimeConfigStore(
                redis, failingMapper, Clock.fixed(NOW, ZoneOffset.UTC));

        assertThatThrownBy(() -> store.update(command("admin:1"), 0))
                .isInstanceOf(BusinessException.class)
                .isInstanceOfSatisfying(BusinessException.class, exception -> {
                    assertThat(exception.getErrorCode().getCode())
                            .isEqualTo("RUNTIME_CONFIG-009");
                    assertThat(exception.getErrorCode().dependency())
                            .isEqualTo(com.kafkick.core.observation.Dependency.NONE);
                });
        verifyNoInteractions(redis);
    }

    private RedisRuntimeConfigStore store(StringRedisTemplate template) {
        return new RedisRuntimeConfigStore(
                template, objectMapper, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private String json(Object value) {
        return objectMapper.writeValueAsString(value);
    }

    private static RuntimeConfigCommand command(String updater) {
        return new RuntimeConfigCommand(EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, updater);
    }

    private static RuntimeConfigSnapshot snapshot(long revision) {
        return new RuntimeConfigSnapshot(
                EngineVersion.V2, ReleaseStage.V2_2, QueueMode.OFF, revision,
                NOW.minusSeconds(60), "seed", SourceStatus.VALID);
    }

    private static final class FakeRedis {
        private final AtomicReference<String> config;
        private final List<String> audits = new ArrayList<>();
        private final StringRedisTemplate template = mock(StringRedisTemplate.class);
        private volatile boolean down;
        private Runnable beforeExecute = () -> { };
        private Runnable beforeGetReturn = () -> { };
        private Runnable beforeAuditRange = () -> { };
        private boolean timeoutAfterCommit;
        private boolean connectionFailureAfterCommit;
        private boolean connectionFailureBeforeCommit;
        private boolean systemFailureAfterCommit;
        private boolean invalidApiUsageBeforeCommit;
        private boolean failReadsAfterUncertainOutcome;

        @SuppressWarnings("unchecked")
        private FakeRedis(ObjectMapper objectMapper, String initial) {
            this.config = new AtomicReference<>(initial);
            ValueOperations<String, String> values = mock(ValueOperations.class);
            @SuppressWarnings("unchecked")
            ListOperations<String, String> lists = mock(ListOperations.class);
            when(template.opsForValue()).thenReturn(values);
            when(template.opsForList()).thenReturn(lists);
            when(lists.range("config:runtime:audit", -1_000, -1))
                    .thenAnswer(invocation -> {
                        beforeAuditRange.run();
                        return List.copyOf(audits);
                    });
            when(values.get(any())).thenAnswer(invocation -> {
                ensureUp();
                String value = config.get();
                beforeGetReturn.run();
                return value;
            });
            when(template.execute(any(), anyList(), any(Object[].class))).thenAnswer(invocation -> {
                ensureUp();
                if (connectionFailureBeforeCommit) {
                    connectionFailureBeforeCommit = false;
                    throw new RedisConnectionFailureException("connection lost before commit");
                }
                if (invalidApiUsageBeforeCommit) {
                    invalidApiUsageBeforeCommit = false;
                    throw new InvalidDataAccessApiUsageException("invalid script invocation");
                }
                beforeExecute.run();
                long expected = Long.parseLong(invocation.getArgument(2));
                synchronized (this) {
                    String current = config.get();
                    if (current == null) {
                        return -2L;
                    }
                    RuntimeConfigSnapshot currentSnapshot;
                    try {
                        currentSnapshot = current == null ? null
                                : objectMapper.readValue(current, RuntimeConfigSnapshot.class);
                    } catch (tools.jackson.core.JacksonException exception) {
                        return -3L;
                    }
                    long revision = currentSnapshot == null ? 0 : currentSnapshot.revision();
                    if (current != null && revision != expected) {
                        return revision;
                    }
                    config.set(invocation.getArgument(3));
                    String requestedJson = invocation.getArgument(4);
                    RuntimeConfigAuditLog requested = objectMapper.readValue(
                            requestedJson, RuntimeConfigAuditLog.class);
                    RuntimeConfigAuditLog actual = new RuntimeConfigAuditLog(
                            revision, requested.newRevision(), currentSnapshot, requested.afterConfig(),
                            requested.changedBy(), requested.changedAt(), requested.requestId());
                    audits.add(objectMapper.writeValueAsString(actual));
                    if (audits.size() > 1_000) {
                        audits.removeFirst();
                    }
                    if (timeoutAfterCommit) {
                        timeoutAfterCommit = false;
                        down = failReadsAfterUncertainOutcome;
                        throw new QueryTimeoutException("result timed out");
                    }
                    if (connectionFailureAfterCommit) {
                        connectionFailureAfterCommit = false;
                        down = failReadsAfterUncertainOutcome;
                        throw new RedisConnectionFailureException("connection lost after commit");
                    }
                    if (systemFailureAfterCommit) {
                        systemFailureAfterCommit = false;
                        throw new RedisSystemException(
                                "command interrupted after commit", new InterruptedException());
                    }
                    return -1L;
                }
            });
        }

        private void ensureUp() {
            if (down) {
                throw new DataAccessResourceFailureException("redis down");
            }
        }
    }
}

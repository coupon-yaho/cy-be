package com.kafkick.core.runtimeconfig;

import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;
import org.junit.jupiter.api.Test;

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

class InMemoryRuntimeConfigStoreTest {

    private static final Instant NOW = Instant.parse("2026-08-20T10:00:00Z");

    @Test
    void inMemoryStoreUsesCasAndReadOnlyStoreRejectsUpdates() {
        RuntimeConfigSnapshot initial = snapshot(0);
        InMemoryRuntimeConfigStore store = new InMemoryRuntimeConfigStore(
                initial, Clock.fixed(NOW, ZoneOffset.UTC));
        RuntimeConfigCommand command = new RuntimeConfigCommand(
                EngineVersion.V3, ReleaseStage.V3, QueueMode.ADAPTIVE, "admin:1");

        assertThat(store.update(command, 0).revision()).isEqualTo(1);
        assertThatThrownBy(() -> store.update(command, 0))
                .isInstanceOfSatisfying(RuntimeConfigRevisionConflictException.class,
                        exception -> assertThat(exception.getCurrentRevision()).isEqualTo(1));
        assertThatThrownBy(() -> new ReadOnlyRuntimeConfigStore(initial).update(command, 0))
                .isInstanceOfSatisfying(BusinessException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(RuntimeConfigErrorCode.READ_ONLY);
                    assertThat(exception.getErrorCode().getStatus()).isEqualTo(409);
                });
    }

    @Test
    void concurrentUpdatesWithSameRevisionHaveExactlyOneWinner() throws Exception {
        int contenders = 16;
        InMemoryRuntimeConfigStore store = new InMemoryRuntimeConfigStore(
                snapshot(0), Clock.fixed(NOW, ZoneOffset.UTC));
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
            assertThat(store.get().revision()).isEqualTo(1);
        }
    }

    @Test
    void storesRejectNullInitialSnapshots() {
        assertThatThrownBy(() -> new InMemoryRuntimeConfigStore(null, Clock.systemUTC()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("initial");
        assertThatThrownBy(() -> new ReadOnlyRuntimeConfigStore(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("snapshot");
    }

    private static RuntimeConfigSnapshot snapshot(long revision) {
        return new RuntimeConfigSnapshot(
                EngineVersion.V2, ReleaseStage.V2_2, QueueMode.OFF, revision,
                NOW.minusSeconds(60), "seed", SourceStatus.VALID);
    }
}

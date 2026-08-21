package com.kafkick.core.runtimeconfig;

import com.kafkick.core.observation.SourceStatus;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class InMemoryRuntimeConfigStore implements RuntimeConfigStore {

    private final AtomicReference<RuntimeConfigSnapshot> current;
    private final Clock clock;

    public InMemoryRuntimeConfigStore(RuntimeConfigSnapshot initial, Clock clock) {
        this.current = new AtomicReference<>(Objects.requireNonNull(initial, "initial"));
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public RuntimeConfigSnapshot get() {
        return current.get();
    }

    @Override
    public RuntimeConfigSnapshot update(RuntimeConfigCommand command, long expectedRevision) {
        while (true) {
            RuntimeConfigSnapshot before = current.get();
            if (before.revision() != expectedRevision) {
                throw new RuntimeConfigRevisionConflictException(before.revision());
            }
            RuntimeConfigSnapshot after = new RuntimeConfigSnapshot(
                    command.engineVersion(), command.releaseStage(), command.queueMode(),
                    expectedRevision + 1, clock.instant(), command.updatedBy(), SourceStatus.VALID);
            if (current.compareAndSet(before, after)) {
                return after;
            }
        }
    }

    @Override
    public Optional<RuntimeConfigSnapshot> getLastKnownGood() {
        return Optional.of(current.get());
    }
}

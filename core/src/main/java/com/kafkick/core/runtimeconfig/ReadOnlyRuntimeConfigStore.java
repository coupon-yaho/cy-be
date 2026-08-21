package com.kafkick.core.runtimeconfig;

import com.kafkick.core.support.exception.BusinessException;

import java.util.Optional;
import java.util.Objects;

public final class ReadOnlyRuntimeConfigStore implements RuntimeConfigStore {

    private final RuntimeConfigSnapshot snapshot;

    public ReadOnlyRuntimeConfigStore(RuntimeConfigSnapshot snapshot) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    }

    @Override
    public RuntimeConfigSnapshot get() {
        return snapshot;
    }

    @Override
    public RuntimeConfigSnapshot update(RuntimeConfigCommand command, long expectedRevision) {
        throw new BusinessException(RuntimeConfigErrorCode.READ_ONLY);
    }

    @Override
    public Optional<RuntimeConfigSnapshot> getLastKnownGood() {
        return Optional.of(snapshot);
    }
}

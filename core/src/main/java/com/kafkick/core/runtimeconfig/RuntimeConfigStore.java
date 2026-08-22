package com.kafkick.core.runtimeconfig;

import java.util.Optional;

public interface RuntimeConfigStore {

    RuntimeConfigSnapshot get();

    RuntimeConfigSnapshot update(RuntimeConfigCommand command, long expectedRevision);

    Optional<RuntimeConfigSnapshot> getLastKnownGood();
}

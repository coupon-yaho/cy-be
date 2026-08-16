package com.kafkick.core.observation;

import java.util.UUID;

public final class UuidEventIdGenerator implements EventIdGenerator {

    @Override
    public UUID generate() {
        return UUID.randomUUID();
    }
}

package com.kafkick.api.observation;

import com.kafkick.core.observation.EventIdGenerator;

import java.util.UUID;

public final class UuidEventIdGenerator implements EventIdGenerator {

    @Override
    public UUID generate() {
        return UUID.randomUUID();
    }
}

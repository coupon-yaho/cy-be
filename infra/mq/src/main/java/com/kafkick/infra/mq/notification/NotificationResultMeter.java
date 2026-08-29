package com.kafkick.infra.mq.notification;

import java.util.Objects;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import com.kafkick.core.observation.DomainMeterNames;

public class NotificationResultMeter {
    public static final String NAME = DomainMeterNames.NOTIFY_SENT;

    private final Counter successes;
    private final Counter failures;

    public NotificationResultMeter(MeterRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        successes = Counter.builder(NAME).tag("result", "success").register(registry);
        failures = Counter.builder(NAME).tag("result", "failure").register(registry);
    }

    public void success() {
        successes.increment();
    }

    public void failure() {
        failures.increment();
    }
}

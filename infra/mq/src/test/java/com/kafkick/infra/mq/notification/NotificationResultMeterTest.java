package com.kafkick.infra.mq.notification;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.kafkick.core.observation.DomainMeterNames;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class NotificationResultMeterTest {
    @Test
    void registersOnlyTheTwoClosedResultSeries() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NotificationResultMeter meter = new NotificationResultMeter(registry);

        meter.success();
        meter.failure();

        assertThat(registry.find(DomainMeterNames.NOTIFY_SENT)
                .tag("result", "success").counter().count()).isEqualTo(1.0d);
        assertThat(registry.find(DomainMeterNames.NOTIFY_SENT)
                .tag("result", "failure").counter().count()).isEqualTo(1.0d);
        assertThat(registry.find(DomainMeterNames.NOTIFY_SENT).counters()).hasSize(2);
    }
}

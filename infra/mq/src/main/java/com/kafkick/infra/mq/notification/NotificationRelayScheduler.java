package com.kafkick.infra.mq.notification;

import java.util.Objects;

import org.springframework.scheduling.annotation.Scheduled;

public class NotificationRelayScheduler {
    private final NotificationOutboxRelay relay;

    public NotificationRelayScheduler(NotificationOutboxRelay relay) {
        this.relay = Objects.requireNonNull(relay, "relay");
    }

    @Scheduled(fixedDelayString = "${kafka.notification.relay.fixed-delay-ms:100}")
    public void relayOne() {
        relay.poll();
    }
}

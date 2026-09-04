package com.kafkick.infra.mq.notification;

import java.util.Objects;

import org.springframework.scheduling.annotation.Scheduled;

public class NotificationRelayScheduler {
    private final NotificationOutboxRelay relay;

    public NotificationRelayScheduler(NotificationOutboxRelay relay) {
        this.relay = Objects.requireNonNull(relay, "relay");
    }

    /**
     * <b>이름이 {@code relayOne} 이 아니다 — 한 건이 아니라 한 회차다.</b>
     * 릴레이가 배치로 집게 된 뒤(CY-902) 한 번에 여러 건이 나간다.
     */
    @Scheduled(fixedDelayString = "${kafka.notification.relay.fixed-delay-ms:100}")
    public void relayBatch() {
        relay.poll();
    }
}

package com.kafkick.infra.mq.notification;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka.notification.relay")
public class NotificationRelayProperties {
    private Duration lease = Duration.ofSeconds(30);
    private long fixedDelayMs = 100L;

    public Duration getLease() {
        return lease;
    }

    public void setLease(Duration lease) {
        if (lease == null || lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("relay lease는 양수여야 합니다.");
        }
        this.lease = lease;
    }

    public long getFixedDelayMs() {
        return fixedDelayMs;
    }

    public void setFixedDelayMs(long fixedDelayMs) {
        if (fixedDelayMs < 1) {
            throw new IllegalArgumentException("relay fixed delay는 양수여야 합니다.");
        }
        this.fixedDelayMs = fixedDelayMs;
    }
}

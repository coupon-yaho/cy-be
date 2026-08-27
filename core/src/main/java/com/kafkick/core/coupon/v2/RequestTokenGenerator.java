package com.kafkick.core.coupon.v2;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class RequestTokenGenerator {

    private final String instanceId;
    private final String bootNonce;
    private final AtomicLong counter;

    public RequestTokenGenerator(String instanceId) {
        this(instanceId, UUID.randomUUID().toString(), 0L);
    }

    RequestTokenGenerator(String instanceId, String bootNonce, long initialCounter) {
        this.instanceId = validateInstanceId(instanceId);
        this.bootNonce = validateBootNonce(bootNonce);
        this.counter = new AtomicLong(initialCounter);
    }

    public String generate() {
        return instanceId
                + "-" + bootNonce
                + "-" + Thread.currentThread().threadId()
                + "-" + counter.getAndIncrement();
    }

    private static String validateInstanceId(String instanceId) {
        Objects.requireNonNull(instanceId, "instanceId");
        if (instanceId.isBlank()) {
            throw new IllegalArgumentException("instanceId는 비어 있을 수 없습니다.");
        }
        if (instanceId.indexOf('|') >= 0) {
            throw new IllegalArgumentException("instanceId에는 '|'를 포함할 수 없습니다.");
        }
        return instanceId;
    }

    private static String validateBootNonce(String bootNonce) {
        Objects.requireNonNull(bootNonce, "bootNonce");
        if (bootNonce.isBlank() || bootNonce.indexOf('|') >= 0) {
            throw new IllegalArgumentException("bootNonce는 비어 있거나 '|'를 포함할 수 없습니다.");
        }
        return bootNonce;
    }
}

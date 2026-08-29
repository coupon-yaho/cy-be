package com.kafkick.infra.redis.runtimeconfig;

import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;

import java.time.Instant;
import java.util.Objects;

public record RuntimeConfigAuditLog(
        long previousRevision,
        long newRevision,
        RuntimeConfigSnapshot beforeConfig,
        RuntimeConfigSnapshot afterConfig,
        String changedBy,
        Instant changedAt,
        String requestId
) {
    public RuntimeConfigAuditLog {
        Objects.requireNonNull(afterConfig, "afterConfig");
        Objects.requireNonNull(changedAt, "changedAt");
        if (changedBy == null || changedBy.isBlank()) {
            throw new IllegalArgumentException("changedBy must not be blank");
        }
        if (requestId == null || requestId.isBlank()) {
            throw new IllegalArgumentException("requestId must not be blank");
        }
    }
}

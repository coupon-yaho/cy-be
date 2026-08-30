package com.kafkick.core.notification.domain;

public enum NotifyFailureReason {
    SEND_TIMEOUT(true),
    SEND_UNAVAILABLE(true),
    CONNECTION_ERROR(true),
    INVALID_RECIPIENT(false),
    REJECTED_BY_PROVIDER(false),
    SERIALIZATION_ERROR(false),
    OUTBOX_PUBLISH_FAILED(false),
    UNKNOWN(false);

    private final boolean retryable;

    NotifyFailureReason(boolean retryable) {
        this.retryable = retryable;
    }

    public boolean retryable() {
        return retryable;
    }
}

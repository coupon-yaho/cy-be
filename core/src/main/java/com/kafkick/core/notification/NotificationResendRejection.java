package com.kafkick.core.notification;

public enum NotificationResendRejection {
    NOT_FOUND("ADMIN-005"),
    CONFLICT("ADMIN-006"),
    LIMIT_EXCEEDED("ADMIN-007");

    private final String code;

    NotificationResendRejection(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}

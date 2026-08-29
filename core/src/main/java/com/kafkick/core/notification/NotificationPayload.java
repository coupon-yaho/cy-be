package com.kafkick.core.notification;

import java.util.Objects;

public record NotificationPayload(String recipientContact, String messageBody) {
    public NotificationPayload {
        Objects.requireNonNull(recipientContact, "recipientContact");
        Objects.requireNonNull(messageBody, "messageBody");
    }

    @Override
    public String toString() {
        return "NotificationPayload[recipientContact=<redacted>, messageBody=<redacted>]";
    }
}

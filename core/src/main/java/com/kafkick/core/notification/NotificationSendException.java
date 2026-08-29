package com.kafkick.core.notification;

import java.util.Objects;

import com.kafkick.core.notification.domain.NotifyFailureReason;

import com.kafkick.core.support.exception.BusinessException;

public class NotificationSendException extends BusinessException {
    private final NotifyFailureReason reason;

    public NotificationSendException(NotifyFailureReason reason, Throwable cause) {
        super(NotificationErrorCode.SEND_FAILED,
                Objects.requireNonNull(reason, "reason").name(), cause);
        this.reason = reason;
    }

    public NotifyFailureReason reason() {
        return reason;
    }
}

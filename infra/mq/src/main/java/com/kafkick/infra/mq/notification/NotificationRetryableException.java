package com.kafkick.infra.mq.notification;

public class NotificationRetryableException extends RuntimeException {
    public NotificationRetryableException(Throwable cause) {
        super("알림 발송을 재시도해야 합니다.", cause);
    }
}

package com.kafkick.infra.mq.notification;

public class NotificationTerminalFailureException extends RuntimeException {
    public NotificationTerminalFailureException(Throwable cause) {
        super("알림 발송이 종결 실패했습니다.", cause);
    }
}

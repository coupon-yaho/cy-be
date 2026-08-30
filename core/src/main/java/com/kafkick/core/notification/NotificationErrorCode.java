package com.kafkick.core.notification;

import com.kafkick.core.support.exception.ErrorCode;

public enum NotificationErrorCode implements ErrorCode {
    ILLEGAL_TRANSITION(409, "NOTIFY-001", "현재 상태에서 알림 상태를 변경할 수 없습니다."),
    RESEND_LIMIT_EXCEEDED(409, "NOTIFY-002", "알림 재발송 횟수 상한을 초과했습니다."),
    SEND_FAILED(503, "NOTIFY-003", "알림 발송 의존성을 사용할 수 없습니다."),
    PAYLOAD_TOO_LARGE(422, "NOTIFY-004", "알림 수신처 또는 본문이 저장 가능한 길이를 초과했습니다."),
    PAYLOAD_REQUIRED(422, "NOTIFY-005", "알림 수신처와 본문은 비어 있을 수 없습니다."),
    NOTIFICATION_NOT_FOUND(404, "NOTIFY-006", "해당 알림을 찾을 수 없습니다."),
    RESEND_CONFLICT(409, "NOTIFY-007", "알림을 재발송할 수 없는 상태이거나 중복 요청입니다.");

    private final int status;
    private final String code;
    private final String message;

    NotificationErrorCode(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    @Override public int getStatus() { return status; }
    @Override public String getCode() { return code; }
    @Override public String getMessage() { return message; }
}

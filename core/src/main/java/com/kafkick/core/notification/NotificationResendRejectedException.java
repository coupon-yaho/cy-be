package com.kafkick.core.notification;

import java.util.Objects;

import com.kafkick.core.support.exception.BusinessException;

/**
 * 수동 재발송 유즈케이스의 정책 거부를 Core 오류 코드와 함께 전달합니다.
 *
 * <p>관리자 HTTP 어댑터는 {@link #rejection()}을 ADMIN-005~007로 변환합니다. 다른 HTTP
 * 어댑터가 별도 변환 없이 공통 예외 처리로 전달해도 {@link BusinessException}의 NOTIFY-*
 * 코드가 500 응답을 막습니다.
 */
public class NotificationResendRejectedException extends BusinessException {
    private final NotificationResendRejection rejection;

    public NotificationResendRejectedException(NotificationResendRejection rejection) {
        super(errorCode(rejection), "알림 재발송 요청이 거부되었습니다: " + rejection);
        this.rejection = rejection;
    }

    public NotificationResendRejection rejection() {
        return rejection;
    }

    private static NotificationErrorCode errorCode(NotificationResendRejection rejection) {
        return switch (Objects.requireNonNull(rejection, "rejection")) {
            case NOT_FOUND -> NotificationErrorCode.NOTIFICATION_NOT_FOUND;
            case CONFLICT -> NotificationErrorCode.RESEND_CONFLICT;
            case LIMIT_EXCEEDED -> NotificationErrorCode.RESEND_LIMIT_EXCEEDED;
        };
    }
}

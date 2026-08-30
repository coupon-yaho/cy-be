package com.kafkick.core.notification.domain;

import com.kafkick.core.notification.NotificationErrorCode;
import com.kafkick.core.support.exception.BusinessException;

public class NotificationResendLimitExceededException extends BusinessException {
    public NotificationResendLimitExceededException(Long notificationId) {
        super(NotificationErrorCode.RESEND_LIMIT_EXCEEDED,
                "알림 재발송 횟수 상한을 초과했습니다: notificationId=" + notificationId);
    }
}

package com.kafkick.core.notification.domain;

import com.kafkick.core.notification.NotificationErrorCode;
import com.kafkick.core.support.exception.BusinessException;

public class NotificationInvalidTransitionException extends BusinessException {

    public NotificationInvalidTransitionException(NotificationStatus from, NotificationStatus to) {
        super(NotificationErrorCode.ILLEGAL_TRANSITION,
                "허용되지 않는 알림 상태 전이입니다: " + from + " -> " + to);
    }
}

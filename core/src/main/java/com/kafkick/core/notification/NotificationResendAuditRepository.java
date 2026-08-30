package com.kafkick.core.notification;

import java.util.Optional;

import com.kafkick.core.notification.domain.NotificationResendAudit;

public interface NotificationResendAuditRepository {
    NotificationResendAudit save(NotificationResendAudit audit);
    Optional<NotificationResendAudit> findLatestAcceptedByNotificationId(Long notificationId);
}

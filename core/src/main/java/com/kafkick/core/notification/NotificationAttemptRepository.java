package com.kafkick.core.notification;

import java.util.List;

import com.kafkick.core.notification.domain.NotificationAttempt;

public interface NotificationAttemptRepository {
    NotificationAttempt save(NotificationAttempt attempt);
    List<NotificationAttempt> findByNotificationId(Long notificationId);
}

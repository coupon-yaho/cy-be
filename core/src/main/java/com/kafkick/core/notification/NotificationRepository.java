package com.kafkick.core.notification;

import java.util.List;
import java.util.Optional;

import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotificationFailure;

public interface NotificationRepository {
    Notification save(Notification notification);
    Optional<Notification> findById(Long notificationId);
    long countByCouponId(Long couponId);
    long countAll();
    long countByCouponIdAndStatusIn(Long couponId, List<NotificationStatus> statuses);
    long countByStatusIn(List<NotificationStatus> statuses);
    List<NotificationFailure> findFailuresBeforeId(Long beforeId, int limit);
    boolean saveIfStatus(Notification notification, NotificationStatus expectedStatus,
            int expectedAttemptCount);
}

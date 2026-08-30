package com.kafkick.storage.db.notification.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kafkick.storage.db.notification.entity.NotificationOutboxEntity;

interface NotificationOutboxJpaRepository extends JpaRepository<NotificationOutboxEntity, Long> {
    Optional<NotificationOutboxEntity> findByNotificationIdAndAttemptSeq(
            Long notificationId, int attemptSeq);
}

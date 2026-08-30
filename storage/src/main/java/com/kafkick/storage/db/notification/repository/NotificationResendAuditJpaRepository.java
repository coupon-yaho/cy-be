package com.kafkick.storage.db.notification.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kafkick.storage.db.notification.entity.NotificationResendAuditEntity;

interface NotificationResendAuditJpaRepository
        extends JpaRepository<NotificationResendAuditEntity, Long> {
    Optional<NotificationResendAuditEntity> findFirstByNotificationIdAndAcceptedTrueOrderByIdDesc(
            Long notificationId);
}

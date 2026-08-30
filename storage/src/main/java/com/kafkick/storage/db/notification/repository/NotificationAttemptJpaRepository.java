package com.kafkick.storage.db.notification.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.kafkick.storage.db.notification.entity.NotificationAttemptEntity;

interface NotificationAttemptJpaRepository extends JpaRepository<NotificationAttemptEntity, Long> {
    List<NotificationAttemptEntity> findByNotificationIdOrderByAttemptSeq(Long notificationId);
}

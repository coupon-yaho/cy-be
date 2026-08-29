package com.kafkick.storage.db.notification.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import com.kafkick.storage.db.notification.entity.NotificationOutboxEntity;

interface NotificationOutboxJpaRepository extends JpaRepository<NotificationOutboxEntity, Long> { }

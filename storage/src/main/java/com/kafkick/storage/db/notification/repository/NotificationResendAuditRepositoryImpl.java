package com.kafkick.storage.db.notification.repository;

import java.util.Optional;

import org.springframework.stereotype.Repository;

import com.kafkick.core.notification.NotificationResendAuditRepository;
import com.kafkick.core.notification.domain.NotificationResendAudit;
import com.kafkick.storage.db.notification.entity.NotificationResendAuditEntity;

@Repository
public class NotificationResendAuditRepositoryImpl implements NotificationResendAuditRepository {
    private final NotificationResendAuditJpaRepository repository;
    public NotificationResendAuditRepositoryImpl(NotificationResendAuditJpaRepository repository) { this.repository = repository; }
    @Override public NotificationResendAudit save(NotificationResendAudit a) {
        if (a.id() != null) {
            throw new IllegalArgumentException("알림 재발송 감사는 변경할 수 없습니다.");
        }
        return toDomain(repository.saveAndFlush(toEntity(a)));
    }
    @Override public Optional<NotificationResendAudit> findLatestAcceptedByNotificationId(Long id) {
        return repository.findFirstByNotificationIdAndAcceptedTrueOrderByIdDesc(id)
                .map(NotificationResendAuditRepositoryImpl::toDomain);
    }
    private static NotificationResendAuditEntity toEntity(NotificationResendAudit a) {
        return new NotificationResendAuditEntity(a.id(), a.notificationId(), a.attemptSeq(), a.requestedBy(),
                a.requestedAt(), a.accepted(), a.rejectCode(), a.createdAt());
    }
    private static NotificationResendAudit toDomain(NotificationResendAuditEntity e) {
        return new NotificationResendAudit(e.getId(), e.getNotificationId(), e.getAttemptSeq(), e.getRequestedBy(),
                e.getRequestedAt(), e.isAccepted(), e.getRejectCode(), e.getCreatedAt());
    }
}

package com.kafkick.storage.db.notification.repository;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.kafkick.core.notification.NotificationAttemptRepository;
import com.kafkick.core.notification.domain.NotificationAttempt;
import com.kafkick.storage.db.notification.entity.NotificationAttemptEntity;

@Repository
public class NotificationAttemptRepositoryImpl implements NotificationAttemptRepository {
    private final NotificationAttemptJpaRepository repository;
    public NotificationAttemptRepositoryImpl(NotificationAttemptJpaRepository repository) { this.repository = repository; }
    @Override public NotificationAttempt save(NotificationAttempt a) {
        if (a.id() != null) {
            throw new IllegalArgumentException("완료된 알림 시도는 변경할 수 없습니다.");
        }
        return toDomain(repository.saveAndFlush(toEntity(a)));
    }
    @Override public List<NotificationAttempt> findByNotificationId(Long id) {
        return repository.findByNotificationIdOrderByAttemptSeq(id).stream()
                .map(NotificationAttemptRepositoryImpl::toDomain).toList();
    }
    private static NotificationAttemptEntity toEntity(NotificationAttempt a) {
        return new NotificationAttemptEntity(a.id(), a.notificationId(), a.attemptSeq(), a.trigger(),
                a.result(), a.failureReason(), a.startedAt(), a.finishedAt(), a.createdAt());
    }
    private static NotificationAttempt toDomain(NotificationAttemptEntity e) {
        return new NotificationAttempt(e.getId(), e.getNotificationId(), e.getAttemptSeq(), e.getTrigger(),
                e.getResult(), e.getFailureReason(), e.getStartedAt(), e.getFinishedAt(), e.getCreatedAt());
    }
}

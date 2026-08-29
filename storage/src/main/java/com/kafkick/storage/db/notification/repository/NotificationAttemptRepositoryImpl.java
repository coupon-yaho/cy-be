package com.kafkick.storage.db.notification.repository;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.kafkick.core.notification.NotificationAttemptRepository;
import com.kafkick.core.notification.domain.NotificationAttempt;
import com.kafkick.storage.db.notification.entity.NotificationAttemptEntity;

@Repository
public class NotificationAttemptRepositoryImpl implements NotificationAttemptRepository {
    private final NotificationAttemptJpaRepository repository;
    private final JdbcTemplate jdbcTemplate;
    public NotificationAttemptRepositoryImpl(NotificationAttemptJpaRepository repository,
            JdbcTemplate jdbcTemplate) {
        this.repository = repository;
        this.jdbcTemplate = jdbcTemplate;
    }
    @Override public NotificationAttempt save(NotificationAttempt a) {
        if (a.id() != null) {
            throw new IllegalArgumentException("완료된 알림 시도는 변경할 수 없습니다.");
        }
        return toDomain(repository.saveAndFlush(toEntity(a)));
    }
    @Override public boolean saveIfAbsent(NotificationAttempt a) {
        if (a.id() != null) {
            throw new IllegalArgumentException("완료된 알림 시도는 새 값만 저장할 수 있습니다.");
        }
        return jdbcTemplate.update("""
                INSERT IGNORE INTO notification_attempts (
                    notification_id, attempt_seq, `trigger`, result, failure_reason,
                    started_at, finished_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, a.notificationId(), a.attemptSeq(), a.trigger().name(), a.result().name(),
                a.failureReason() == null ? null : a.failureReason().name(),
                Timestamp.from(a.startedAt()), Timestamp.from(a.finishedAt()),
                Timestamp.from(a.createdAt())) == 1;
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

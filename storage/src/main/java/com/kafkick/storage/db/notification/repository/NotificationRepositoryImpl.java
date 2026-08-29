package com.kafkick.storage.db.notification.repository;

import java.util.List;
import java.util.Optional;

import jakarta.persistence.EntityManager;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotificationFailure;
import com.kafkick.storage.db.notification.entity.NotificationEntity;

@Repository
public class NotificationRepositoryImpl implements NotificationRepository {
    private final NotificationJpaRepository repository;
    private final EntityManager entityManager;
    public NotificationRepositoryImpl(NotificationJpaRepository repository, EntityManager entityManager) {
        this.repository = repository;
        this.entityManager = entityManager;
    }

    @Override public Notification save(Notification notification) {
        if (notification.id() != null) {
            throw new IllegalArgumentException("기존 알림은 CAS로만 변경할 수 있습니다.");
        }
        return toDomain(repository.saveAndFlush(toEntity(notification)));
    }
    @Override public Optional<Notification> findById(Long id) { return repository.findById(id).map(NotificationRepositoryImpl::toDomain); }
    @Override public long countByCouponId(Long id) { return repository.countByCouponId(id); }
    @Override public long countAll() { return repository.count(); }
    @Override public long countByCouponIdAndStatusIn(Long id, List<NotificationStatus> statuses) {
        return repository.countByCouponIdAndStatusIn(id, statuses);
    }
    @Override public long countByStatusIn(List<NotificationStatus> statuses) {
        return repository.countByStatusIn(statuses);
    }
    @Override public List<NotificationFailure> findFailuresBeforeId(Long beforeId, int limit) {
        List<NotificationStatus> statuses = List.of(NotificationStatus.FAILED, NotificationStatus.DEAD);
        return beforeId == null
                ? repository.findFailures(statuses, PageRequest.of(0, limit))
                : repository.findFailuresBeforeId(statuses, beforeId, PageRequest.of(0, limit));
    }
    @Override
    @Transactional
    public boolean saveIfStatus(Notification notification, NotificationStatus expectedStatus,
            int expectedAttemptCount) {
        int attemptIncrement = notification.attemptCount() - expectedAttemptCount;
        if (attemptIncrement < 0 || attemptIncrement > 1) {
            throw new IllegalArgumentException("시도 횟수는 CAS에서 1만 증가할 수 있습니다.");
        }
        Notification current = findById(notification.id()).orElse(null);
        if (current == null || current.status() != expectedStatus
                || current.attemptCount() != expectedAttemptCount) {
            return false;
        }
        int resendIncrement = notification.resendCount() - current.resendCount();
        if (resendIncrement < 0 || resendIncrement > 1) {
            throw new IllegalArgumentException("재발송 횟수는 CAS에서 1만 증가할 수 있습니다.");
        }
        boolean updated = repository.updateIfStatus(
                toEntity(notification), expectedStatus, expectedAttemptCount,
                attemptIncrement, resendIncrement) == 1;
        entityManager.detach(entityManager.getReference(NotificationEntity.class, notification.id()));
        return updated;
    }
    private static NotificationEntity toEntity(Notification n) {
        return new NotificationEntity(n.id(), n.couponId(), n.memberId(), n.issuanceId(), n.channel(),
                n.status(), n.attemptCount(), n.resendCount(), n.lastFailureReason(), n.recipientContact(),
                n.messageBody(), n.createdAt(), n.updatedAt(), n.sentAt(), n.failedAt());
    }
    private static Notification toDomain(NotificationEntity e) {
        return new Notification(e.getId(), e.getCouponId(), e.getMemberId(), e.getIssuanceId(),
                e.getChannel(), e.getStatus(), e.getAttemptCount(), e.getResendCount(),
                e.getLastFailureReason(), e.getRecipientContact(), e.getMessageBody(), e.getCreatedAt(),
                e.getUpdatedAt(), e.getSentAt(), e.getFailedAt());
    }
}

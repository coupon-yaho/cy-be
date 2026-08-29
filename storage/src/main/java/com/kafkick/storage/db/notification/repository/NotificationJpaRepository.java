package com.kafkick.storage.db.notification.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotificationFailure;
import com.kafkick.storage.db.notification.entity.NotificationEntity;

interface NotificationJpaRepository extends JpaRepository<NotificationEntity, Long> {
    long countByCouponId(Long couponId);
    long countByCouponIdAndStatusIn(Long couponId, List<NotificationStatus> statuses);
    long countByStatusIn(List<NotificationStatus> statuses);
    @Query("""
            SELECT new com.kafkick.core.notification.domain.NotificationFailure(
                notification.id, notification.couponId, notification.memberId,
                notification.lastFailureReason, notification.attemptCount, notification.failedAt)
              FROM NotificationEntity notification
             WHERE notification.status IN :statuses AND notification.id < :beforeId
             ORDER BY notification.id DESC
            """)
    List<NotificationFailure> findFailuresBeforeId(@Param("statuses") List<NotificationStatus> statuses,
            @Param("beforeId") Long beforeId, Pageable pageable);

    @Query("""
            SELECT new com.kafkick.core.notification.domain.NotificationFailure(
                notification.id, notification.couponId, notification.memberId,
                notification.lastFailureReason, notification.attemptCount, notification.failedAt)
              FROM NotificationEntity notification
             WHERE notification.status IN :statuses
             ORDER BY notification.id DESC
            """)
    List<NotificationFailure> findFailures(@Param("statuses") List<NotificationStatus> statuses,
            Pageable pageable);

    @Modifying(flushAutomatically = true)
    @Query("""
            UPDATE NotificationEntity notification
               SET notification.status = :#{#next.status},
                   notification.attemptCount = notification.attemptCount + :attemptIncrement,
                   notification.resendCount = notification.resendCount + :resendIncrement,
                   notification.lastFailureReason = :#{#next.lastFailureReason},
                   notification.updatedAt = :#{#next.updatedAt},
                   notification.sentAt = :#{#next.sentAt},
                   notification.failedAt = :#{#next.failedAt}
             WHERE notification.id = :#{#next.id}
               AND notification.status = :expected
               AND notification.attemptCount = :expectedAttemptCount
            """)
    int updateIfStatus(@Param("next") NotificationEntity next,
            @Param("expected") NotificationStatus expected,
            @Param("expectedAttemptCount") int expectedAttemptCount,
            @Param("attemptIncrement") int attemptIncrement,
            @Param("resendIncrement") int resendIncrement);
}

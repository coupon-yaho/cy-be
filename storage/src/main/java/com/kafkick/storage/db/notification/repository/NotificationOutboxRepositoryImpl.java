package com.kafkick.storage.db.notification.repository;

import java.time.Instant;
import java.time.Duration;
import java.sql.Timestamp;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationOutboxStatus;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.storage.db.notification.entity.NotificationOutboxEntity;

@Repository
public class NotificationOutboxRepositoryImpl implements NotificationOutboxRepository {
    private static final long EXPIRED_CLAIM_RETRY_DELAY_SECONDS = 1;

    private final NotificationOutboxJpaRepository repository;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate requiresNew;

    public NotificationOutboxRepositoryImpl(
            NotificationOutboxJpaRepository repository,
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager
    ) {
        this.repository = repository;
        this.jdbcTemplate = jdbcTemplate;
        this.requiresNew = new TransactionTemplate(transactionManager);
        this.requiresNew.setPropagationBehavior(Propagation.REQUIRES_NEW.value());
    }

    @Override
    public NotificationOutbox save(NotificationOutbox outbox) {
        if (outbox.id() != null || outbox.status() != NotificationOutboxStatus.PENDING) {
            throw new IllegalArgumentException("발행 명령은 상태 전이 메서드로만 변경할 수 있습니다.");
        }
        return toDomain(repository.saveAndFlush(toEntity(outbox)));
    }

    @Override
    public Optional<AttemptTrigger> findTriggerByNotificationIdAndAttemptSeq(
            Long notificationId, int attemptSeq) {
        return repository.findByNotificationIdAndAttemptSeq(notificationId, attemptSeq)
                .map(NotificationOutboxEntity::getTrigger);
    }

    @Override
    public Optional<NotificationOutboxClaim> claimNext(Duration lease) {
        long leaseSeconds = durationSeconds(lease, true, "outbox lease");
        try {
            requiresNew.executeWithoutResult(ignored -> recoverExpiredClaim(leaseSeconds));
            return requiresNew.execute(ignored -> claimPending());
        } catch (PessimisticLockingFailureException contention) {
            return Optional.empty();
        }
    }

    private Optional<NotificationOutboxClaim> claimPending() {
        String token = UUID.randomUUID().toString();
        int updated = jdbcTemplate.update("""
                    UPDATE notification_outbox
                       SET status='IN_PROGRESS', claimed_at=CURRENT_TIMESTAMP(6), claim_token=?
                     WHERE status='PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP(6)
                     ORDER BY next_attempt_at, id
                     LIMIT 1
                    """, token);
        if (updated != 1) return Optional.empty();
        return jdbcTemplate.query("""
                SELECT id, notification_id, attempt_seq, `trigger`, claim_token, created_at
                  FROM notification_outbox WHERE claim_token=?
                """, (rs, row) -> new NotificationOutboxClaim(rs.getLong("id"),
                        rs.getLong("notification_id"), rs.getInt("attempt_seq"),
                        AttemptTrigger.valueOf(rs.getString("trigger")), rs.getString("claim_token"),
                        rs.getTimestamp("created_at").toInstant()), token)
                .stream().findFirst();
    }

    private void recoverExpiredClaim(long leaseSeconds) {
        record ExpiredClaim(long id, int failureCount) { }
        Optional<ExpiredClaim> expired = jdbcTemplate.query("""
                SELECT id, failure_count
                  FROM notification_outbox
                 WHERE status='IN_PROGRESS'
                   AND claimed_at < TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6))
                 ORDER BY claimed_at, id
                 LIMIT 1
                """, (rs, row) -> new ExpiredClaim(rs.getLong("id"), rs.getInt("failure_count")),
                -leaseSeconds).stream().findFirst();
        if (expired.isEmpty()) return;
        ExpiredClaim claim = expired.orElseThrow();
        int nextFailureCount = claim.failureCount() + 1;
        String nextStatus = nextFailureCount >= 10 ? "DEAD" : "PENDING";
        int updated = jdbcTemplate.update("""
                UPDATE notification_outbox
                   SET failure_count=?, status=?,
                       next_attempt_at=TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6)),
                       claimed_at=NULL, claim_token=NULL
                 WHERE id=? AND status='IN_PROGRESS' AND failure_count=?
                   AND claimed_at < TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6))
                """, nextFailureCount, nextStatus, EXPIRED_CLAIM_RETRY_DELAY_SECONDS,
                claim.id(), claim.failureCount(), -leaseSeconds);
        if (updated == 1 && nextFailureCount >= 10) {
            failManualNotification(claim.id());
        }
    }

    private static long durationSeconds(Duration duration, boolean positive, String name) {
        if (duration == null || duration.getNano() != 0
                || (positive ? duration.compareTo(Duration.ofSeconds(1)) < 0 : duration.isNegative())
                || duration.compareTo(Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException(name + "는 지원 범위의 정수 초여야 합니다.");
        }
        return duration.getSeconds();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markPublished(Long outboxId, String claimToken, Instant publishedAt) {
        boolean updated = jdbcTemplate.update("""
                UPDATE notification_outbox
                   SET status='PUBLISHED', published_at=?, claimed_at=NULL, claim_token=NULL
                 WHERE id=? AND status='IN_PROGRESS' AND claim_token=?
                """, Timestamp.from(publishedAt), outboxId, claimToken) == 1;
        return updated;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markFailed(Long outboxId, String claimToken, Duration retryDelay) {
        long retrySeconds = durationSeconds(retryDelay, false, "outbox retry delay");
        Optional<Integer> current = jdbcTemplate.query("""
                SELECT failure_count FROM notification_outbox
                 WHERE id=? AND status='IN_PROGRESS' AND claim_token=?
                """, (rs, row) -> rs.getInt("failure_count"), outboxId, claimToken)
                .stream().findFirst();
        if (current.isEmpty()) return false;
        int nextFailureCount = current.orElseThrow() + 1;
        String nextStatus = nextFailureCount >= 10 ? "DEAD" : "PENDING";
        boolean updated = jdbcTemplate.update("""
                UPDATE notification_outbox
                   SET failure_count=?, status=?,
                       next_attempt_at=TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6)),
                       claimed_at=NULL, claim_token=NULL
                 WHERE id=? AND status='IN_PROGRESS' AND claim_token=? AND failure_count=?
                """, nextFailureCount, nextStatus, retrySeconds,
                outboxId, claimToken, current.orElseThrow()) == 1;
        if (updated && nextFailureCount >= 10) {
            failManualNotification(outboxId);
        }
        return updated;
    }

    private void failManualNotification(Long outboxId) {
        int won = jdbcTemplate.update("""
                INSERT IGNORE INTO notification_attempts (
                    notification_id, attempt_seq, `trigger`, result, failure_reason,
                    started_at, finished_at, created_at
                )
                SELECT o.notification_id, o.attempt_seq, 'MANUAL', 'FAILED', 'OUTBOX_PUBLISH_FAILED',
                       CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                  FROM notification_outbox o
                  JOIN notifications n ON n.id=o.notification_id
                 WHERE o.id=? AND o.status='DEAD' AND o.`trigger`='MANUAL'
                   AND n.status='SENDING' AND n.attempt_count=o.attempt_seq AND n.resend_count > 0
                """, outboxId);
        if (won != 1) return;
        jdbcTemplate.update("""
                UPDATE notifications n
                JOIN notification_outbox o ON o.notification_id=n.id
                   SET n.status='FAILED', n.last_failure_reason='OUTBOX_PUBLISH_FAILED',
                       n.resend_count=n.resend_count-1,
                       n.failed_at=CURRENT_TIMESTAMP(6), n.updated_at=CURRENT_TIMESTAMP(6)
                 WHERE o.id=? AND o.status='DEAD' AND o.trigger='MANUAL'
                   AND n.status='SENDING' AND n.attempt_count=o.attempt_seq AND n.resend_count > 0
                """, outboxId);
    }

    private static NotificationOutboxEntity toEntity(NotificationOutbox outbox) {
        return new NotificationOutboxEntity(outbox.id(), outbox.notificationId(), outbox.attemptSeq(),
                outbox.trigger(), outbox.status(), 0, null, null, null,
                outbox.createdAt(), outbox.publishedAt());
    }

    private static NotificationOutbox toDomain(NotificationOutboxEntity entity) {
        return new NotificationOutbox(entity.getId(), entity.getNotificationId(), entity.getAttemptSeq(),
                entity.getTrigger(), entity.getStatus(), entity.getCreatedAt(), entity.getPublishedAt());
    }
}

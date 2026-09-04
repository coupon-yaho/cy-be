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
    /**
     * <b>여기는 아직 고정 1초다 — 발행 실패 경로만 흩뜨렸다(CY-903).</b>
     *
     * <p>같은 결함이 남아 있고, 사실 <b>이쪽이 더 잘 뭉친다</b>. 발행 실패는 확률적으로
     * 흩어져 일어나지만 lease 만료는 <b>릴레이가 죽거나 재기동이 느릴 때 인플라이트가
     * 한꺼번에</b> 만료되기 때문이다. 그것들이 전부 같은 1초 창으로 돌아온다.
     *
     * <p><b>같이 안 고친 이유는 한 줄이 아니어서다.</b> 이 메서드는 어댑터 안이라
     * {@code FullJitterBackOff} 에 닿지 못한다. 백오프를 어댑터에 넘기면 CY-903 이 세운
     * <i>"지연 정책은 릴레이에 둔다"</i> 를 부분적으로 되돌리므로, 어디에 두어야 두 경로가
     * <b>한 벌</b>을 공유하는지가 먼저 정해져야 한다. 그 결정을 재시도 로직 리뷰에 섞으면
     * 둘 다 흐려진다.
     *
     * <p>티켓 — <a href="https://github.com/coupon-yaho/cy-be/issues/196">#196 (CY-907)</a>
     */
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
                SELECT id, notification_id, attempt_seq, `trigger`, claim_token, created_at,
                       failure_count
                  FROM notification_outbox WHERE claim_token=?
                """, (rs, row) -> new NotificationOutboxClaim(rs.getLong("id"),
                        rs.getLong("notification_id"), rs.getInt("attempt_seq"),
                        AttemptTrigger.valueOf(rs.getString("trigger")), rs.getString("claim_token"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getInt("failure_count")), token)
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

    /**
     * <b>재시도 지연은 초로 자르면 안 된다.</b> Full Jitter 의 첫 재시도 상한이
     * {@code base=200ms} 기준 400ms 라, 초로 자르면 전부 0 이 되어 실패한 것들이
     * <b>즉시 동시에</b> 다시 온다 — 흩뜨리려고 넣은 지터가 오히려 뭉치게 만든다.
     *
     * <p>{@code next_attempt_at} 이 {@code datetime(6)} 이므로 마이크로초까지 표현된다.
     * lease 는 여전히 {@link #durationSeconds} 를 쓴다 — 그쪽은 30초 단위라 정밀도가 필요 없고,
     * 음수로 뒤집어 쓰는 자리라 계산이 단순한 편이 낫다.
     *
     * <p><b>0 은 받는다.</b> Full Jitter 의 하한이 0 이고, 그것이 하한을 두지 않는 이유다.
     *
     * @throws IllegalArgumentException {@code null}·음수이거나 365일을 넘을 때.
     *         <b>이것은 쓰기 시점에 터진다</b> — 설정에서 온 값이라면 그 전에
     *         {@code NotificationRelayProperties} 가 기동 시점에 걸러야 한다
     */
    private static long durationMicros(Duration duration, String name) {
        if (duration == null || duration.isNegative()
                || duration.compareTo(Duration.ofDays(365)) > 0) {
            throw new IllegalArgumentException(name + "는 0 이상 365일 이하여야 합니다.");
        }
        return duration.toNanos() / 1_000L;
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
        long retryMicros = durationMicros(retryDelay, "outbox retry delay");
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
                       next_attempt_at=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6)),
                       claimed_at=NULL, claim_token=NULL
                 WHERE id=? AND status='IN_PROGRESS' AND claim_token=? AND failure_count=?
                """, nextFailureCount, nextStatus, retryMicros,
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

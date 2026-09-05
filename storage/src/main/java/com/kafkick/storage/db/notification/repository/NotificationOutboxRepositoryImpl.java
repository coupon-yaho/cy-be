package com.kafkick.storage.db.notification.repository;

import java.time.Instant;
import java.time.Duration;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.OutboxRetryReason;
import com.kafkick.core.notification.retry.FullJitterBackOff;
import com.kafkick.core.notification.domain.NotificationOutboxStatus;
import com.kafkick.core.notification.domain.NotificationOutboxClaim;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.storage.db.notification.entity.NotificationOutboxEntity;

@Repository
public class NotificationOutboxRepositoryImpl implements NotificationOutboxRepository {
    /**
     * 이 횟수째 실패에서 종착시킨다. <b>여기 하나에만 적는다</b> — 만료 회수도 같은 값을
     * 쓰는데, 두 벌로 두면 한쪽만 고쳐질 때 <b>경로에 따라 종착 시점이 달라진다.</b>
     */
    private static final int DEAD_AFTER_FAILURES = 10;


    /**
     * 백로그를 상태 하나씩 세는 질의.
     *
     * <p><b>패키지 가시성이다</b> — {@code BacklogPlanContractTest} 가 <b>이 문자열 그대로</b>
     * {@code EXPLAIN} 을 건다. 테스트가 자기 SQL 을 적어 두면 어댑터를 {@code IN} 하나로
     * 되돌려도 <b>테스트는 그대로 통과한다</b>(실제로 그랬다).
     */
    static final String COUNT_BY_STATUS =
            "SELECT COUNT(*) FROM notification_outbox WHERE status = ?";

    private final NotificationOutboxJpaRepository repository;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate requiresNew;

    /**
     * <b>이 어댑터는 지연 정책을 소유하지 않는다 — 주입받아 쓸 뿐이다.</b>
     *
     * <p>한때 여기 {@code EXPIRED_CLAIM_RETRY_DELAY_SECONDS = 1} 이 있었다. 발행 실패
     * 경로만 흩뜨리고(CY-903) 이쪽은 고정 1초로 남겨 뒀는데, <b>사실 이쪽이 더 잘
     * 뭉친다</b> — 발행 실패는 확률적으로 흩어져 나지만 lease 만료는 릴레이가 죽거나
     * 재기동이 느릴 때 <b>인플라이트가 한꺼번에</b> 만료되고, 그것들이 전부 같은 1초
     * 창으로 돌아왔다.
     *
     * <p>정책이 {@code core} 에 있는 이유와 후보 셋 중 무엇을 왜 골랐는지는
     * {@code NotificationRetryBackOffConfig} 에 적었다.
     */
    private final FullJitterBackOff backOff;

    /**
     * 이 어댑터 안에서만 일어나는 일을 센다 — lease 만료 회수와 {@code DEAD} 전이.
     * 왜 릴레이가 못 세는지는 {@link NotificationOutboxMeter} 에 적었다.
     */
    private final NotificationOutboxMeter meter;

    /**
     * @throws NullPointerException {@code backOff} 나 {@code meter} 가 {@code null} 일 때.
     *         <b>여기서 막는 이유</b> — 안 막으면 그 사실이 <b>lease 가 처음 만료되는
     *         순간</b>에야 드러난다. 그때는 회수가 통째로 실패하고, 인플라이트가 아무도
     *         못 집는 상태로 쌓인다. 기동 시점으로 당긴다
     */
    public NotificationOutboxRepositoryImpl(
            NotificationOutboxJpaRepository repository,
            JdbcTemplate jdbcTemplate,
            PlatformTransactionManager transactionManager,
            FullJitterBackOff backOff,
            NotificationOutboxMeter meter
    ) {
        this.repository = repository;
        this.jdbcTemplate = jdbcTemplate;
        this.requiresNew = new TransactionTemplate(transactionManager);
        this.requiresNew.setPropagationBehavior(Propagation.REQUIRES_NEW.value());
        this.backOff = Objects.requireNonNull(backOff, "backOff");
        this.meter = Objects.requireNonNull(meter, "meter");
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

    /**
     * @throws IllegalArgumentException {@code max} 가 1 미만이거나 {@code lease} 가
     *         정수 초가 아닐 때
     */
    @Override
    public List<NotificationOutboxClaim> claimBatch(Duration lease, int max) {
        if (max < 1) {
            throw new IllegalArgumentException(
                    "claim batch size 는 1 이상이어야 합니다. 0 이면 LIMIT 0 이 오류 없이 "
                            + "0건을 돌려줘 릴레이가 조용히 멈춥니다. 받은 값=" + max);
        }
        long leaseSeconds = durationSeconds(lease, true, "outbox lease");
        try {
            requiresNew.executeWithoutResult(ignored -> recoverExpiredClaims(leaseSeconds, max));
            return requiresNew.execute(ignored -> claimPending(max));
        } catch (PessimisticLockingFailureException contention) {
            // SKIP LOCKED 가 대부분을 막지만 회수 경로는 여전히 기다린다. 이번 회차를 접는다.
            return List.of();
        }
    }

    /**
     * <b>먼저 잠그고, 잠근 것만 선점 표시한다.</b>
     *
     * <p>한 문장으로 {@code UPDATE ... LIMIT n} 을 쓰면 갱신 대상을 찾는 동안 남이 잠근 행에서
     * 멈춘다. {@code SKIP LOCKED} 는 그 행을 <b>조용히 결과에서 빼므로</b> 워커가 서로를
     * 기다리지 않는다 — MySQL 레퍼런스가 큐 테이블 용도로 지목한 바로 그 성질이다.
     *
     * <p><b>토큰은 배치가 아니라 행마다 다르다.</b> {@code uk_notification_outbox_claim_token}
     * 이 유일 제약이라 한 배치에 같은 토큰을 쓰면 두 번째 행에서 중복키로 죽는다.
     * 게다가 토큰은 <b>펜싱</b> 수단이라 행마다 다른 편이 맞다 — 늦게 돌아온 워커가 자기
     * 것만 못 쓰게 되어야지, 배치 전체를 무효로 만들면 안 된다.
     *
     * <p>⚠️ {@code SKIP LOCKED} 는 결과가 비결정적이라 <b>statement-based replication 에
     * unsafe</b> 다(MySQL 문서 명시). 이 저장소는 {@code BinlogFormatGuard} 가 ROW 포맷을
     * 확인하므로 전제가 지켜진다 — 그 가드가 이제 이 질의의 선행조건이기도 하다.
     */
    private List<NotificationOutboxClaim> claimPending(int max) {
        List<Long> ids = jdbcTemplate.query("""
                SELECT id FROM notification_outbox
                 WHERE status='PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP(6)
                 ORDER BY next_attempt_at, id
                 LIMIT ?
                   FOR UPDATE SKIP LOCKED
                """, (rs, row) -> rs.getLong("id"), max);
        if (ids.isEmpty()) {
            return List.of();
        }

        List<Object[]> args = new ArrayList<>(ids.size());
        for (Long id : ids) {
            args.add(new Object[] {UUID.randomUUID().toString(), id});
        }
        jdbcTemplate.batchUpdate("""
                UPDATE notification_outbox
                   SET status='IN_PROGRESS', claimed_at=CURRENT_TIMESTAMP(6), claim_token=?
                 WHERE id=? AND status='PENDING'
                """, args);

        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        return jdbcTemplate.query("""
                SELECT id, notification_id, attempt_seq, `trigger`, claim_token, created_at,
                       failure_count
                  FROM notification_outbox
                 WHERE id IN (%s) AND status='IN_PROGRESS'
                 ORDER BY next_attempt_at, id
                """.formatted(placeholders),
                (rs, row) -> new NotificationOutboxClaim(rs.getLong("id"),
                        rs.getLong("notification_id"), rs.getInt("attempt_seq"),
                        AttemptTrigger.valueOf(rs.getString("trigger")), rs.getString("claim_token"),
                        rs.getTimestamp("created_at").toInstant(),
                        rs.getInt("failure_count")),
                ids.toArray());
    }

    /**
     * lease 가 지난 선점을 되돌린다. <b>한 번에 여러 건</b> 본다.
     *
     * <p>한 건씩 되돌리면 <b>릴레이가 죽었을 때 회복이 가장 느리다</b> — 그때는 인플라이트가
     * 한꺼번에 만료되는데, 주기마다 한 건씩만 걷으면 밀린 만큼 주기가 곱해진다.
     *
     * <p>선점 표시와 달리 여기는 {@code SKIP LOCKED} 를 쓰지 않는다.
     * <b>lease 만료는 애플리케이션 시간 조건이지 DB 락이 없다는 뜻이 아니다</b> —
     * 만료된 행을 다른 트랜잭션이 그 순간 잡고 있을 수 있고, 그래서 이 경로는 여전히
     * {@code PessimisticLockingFailureException} 을 만날 수 있다(부르는 쪽이 잡아 접는다).
     *
     * <p>그럼에도 안 쓰는 이유는 <b>정확성이 이미 다른 데서 나오기 때문</b>이다 —
     * 조건부 갱신({@code AND failure_count=?})이 동시 회수를 가르므로 둘이 부딪혀도
     * 한쪽만 이긴다. 여기서 건너뛰면 오히려 회수가 밀린다.
     *
     * <p>지연은 <b>건마다 따로</b> 뽑는다. 한 번 뽑아 배치 전체에 쓰면 <b>지터를 넣고도
     * 같이 만료된 것들이 여전히 같은 시각으로 돌아온다</b> — 이 티켓이 고치려는 것이
     * 정확히 그 뭉침이라, 값을 재사용하는 순간 고친 게 없어진다.
     */
    private void recoverExpiredClaims(long leaseSeconds, int max) {
        record ExpiredClaim(long id, int failureCount) { }
        List<ExpiredClaim> expired = jdbcTemplate.query("""
                SELECT id, failure_count
                  FROM notification_outbox
                 WHERE status='IN_PROGRESS'
                   AND claimed_at < TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6))
                 ORDER BY claimed_at, id
                 LIMIT ?
                """, (rs, row) -> new ExpiredClaim(rs.getLong("id"), rs.getInt("failure_count")),
                -leaseSeconds, max);

        // **센 것을 모아 뒀다가 커밋 뒤에 낸다.** 이 트랜잭션은 여러 행을 고치므로, 뒤쪽
        // 행의 오류가 앞쪽까지 되돌려도 미터는 트랜잭션을 안 타 숫자만 그대로 남는다.
        List<Duration> recovered = new ArrayList<>();
        int deaths = 0;
        for (ExpiredClaim claim : expired) {
            int nextFailureCount = claim.failureCount() + 1;
            boolean dead = nextFailureCount >= DEAD_AFTER_FAILURES;
            String nextStatus = dead ? "DEAD" : "PENDING";
            // 지연은 **건마다 따로** 뽑는다. 한 번 뽑아 배치 전체에 쓰면 지터를 넣고도
            // 같이 만료된 것들이 여전히 같은 시각으로 돌아온다.
            Duration retryDelay = backOff.nextDelay(nextFailureCount);
            long retryMicros = durationMicros(retryDelay, "expired claim retry delay");
            int updated = jdbcTemplate.update("""
                    UPDATE notification_outbox
                       SET failure_count=?, status=?,
                           next_attempt_at=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6)),
                           claimed_at=NULL, claim_token=NULL
                     WHERE id=? AND status='IN_PROGRESS' AND failure_count=?
                       AND claimed_at < TIMESTAMPADD(SECOND, ?, CURRENT_TIMESTAMP(6))
                    """, nextFailureCount, nextStatus, retryMicros,
                    claim.id(), claim.failureCount(), -leaseSeconds);
            if (updated != 1) {
                // 조건부 갱신이라 남이 먼저 회수하면 0행이다. 그때 세면 지표가 실제
                // 회수보다 커진다.
                //
                // ⚠️ **이 분기는 테스트가 못 태운다.** 0행이 되려면 SELECT 와 UPDATE 사이에
                //    남이 같은 행을 회수해야 하는데, 그 끼어듦을 확실히 일으킬 방법이 없다.
                //    두 스레드로 돌려 보는 테스트는 끼어듦이 안 나면 조용히 통과하므로
                //    없는 것만 못하다. 그 사실을 숨기지 않고 적어 둔다.
                continue;
            }
            if (dead) {
                failManualNotification(claim.id());
                deaths++;
            } else {
                recovered.add(retryDelay);
            }
        }

        int deadCount = deaths;
        afterCommit(() -> {
            recovered.forEach(delay -> meter.retried(OutboxRetryReason.LEASE_EXPIRED, delay));
            for (int i = 0; i < deadCount; i++) {
                meter.dead(OutboxRetryReason.LEASE_EXPIRED);
            }
        });
    }

    /**
     * 커밋된 <b>뒤에</b> 돌린다. 트랜잭션 밖이면 그 자리에서 돌린다.
     *
     * <p>미터는 트랜잭션을 안 탄다 — 쓰기 전에 세면 롤백돼도 지표에는 남고, 그 숫자를
     * 보는 사람은 일어나지 않은 일을 본다.
     *
     * <p><b>패키지 가시성인 이유</b> — 롤백돼도 안 도는지를 직접 태워 보려고 열었다.
     * {@code private} 로 두면 그 분기를 태울 방법이 없고, 실제로 처음에는
     * 돌연변이(커밋 전에 세기)가 <b>안 잡혔다.</b>
     */
    static void afterCommit(Runnable action) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            action.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        action.run();
                    }
                });
    }

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

    /**
     * @throws NullPointerException {@code reason} 이 {@code null} 일 때. <b>쓰기 전에</b>
     *         막는다 — 뒤에서 터지면 상태는 이미 바뀌었는데 그 사실이 지표에 안 남아,
     *         되돌려진 건수와 세어진 건수가 조용히 어긋난다
     * @throws IllegalArgumentException {@code retryDelay} 가 음수이거나 365일을 넘을 때
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean markFailed(Long outboxId, String claimToken, Duration retryDelay,
            OutboxRetryReason reason) {
        Objects.requireNonNull(reason, "reason");
        long retryMicros = durationMicros(retryDelay, "outbox retry delay");
        Optional<Integer> current = jdbcTemplate.query("""
                SELECT failure_count FROM notification_outbox
                 WHERE id=? AND status='IN_PROGRESS' AND claim_token=?
                """, (rs, row) -> rs.getInt("failure_count"), outboxId, claimToken)
                .stream().findFirst();
        if (current.isEmpty()) return false;
        int nextFailureCount = current.orElseThrow() + 1;
        boolean dead = nextFailureCount >= DEAD_AFTER_FAILURES;
        String nextStatus = dead ? "DEAD" : "PENDING";
        boolean updated = jdbcTemplate.update("""
                UPDATE notification_outbox
                   SET failure_count=?, status=?,
                       next_attempt_at=TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(6)),
                       claimed_at=NULL, claim_token=NULL
                 WHERE id=? AND status='IN_PROGRESS' AND claim_token=? AND failure_count=?
                """, nextFailureCount, nextStatus, retryMicros,
                outboxId, claimToken, current.orElseThrow()) == 1;
        if (!updated) {
            // 남이 먼저 가져갔다. **아무것도 세지 않는다** — 이 건은 이미 남의 것이다.
            return false;
        }
        if (dead) {
            failManualNotification(outboxId);
        }
        afterCommit(() -> {
            if (dead) {
                meter.dead(reason);
            } else {
                meter.retried(reason, retryDelay);
            }
        });
        return true;
    }

    /**
     * <b>{@code failure_count} 를 안 건드린다.</b> 이 경로는 발행이 실패한 것이 아니라
     * <b>시작도 못 한 것</b>이다 — 워커 풀이 제출을 거부한 건을 실패로 세면, 거부가 잦은
     * 순간에 {@code failure_count} 가 실제 발행 실패 없이 10 에 닿아 <b>한 번도 안 보낸
     * 알림이 {@code DEAD} 가 된다.</b>
     *
     * <p>{@code next_attempt_at} 을 현재 시각으로 둔다. 지연을 주면 되돌린 의미가 없다 —
     * 이 건은 아직 아무 대가도 치르지 않았다.
     *
     * <p>{@code REQUIRES_NEW} 인 이유는 {@link #markFailed}·{@link #markPublished} 와 같다 —
     * 부르는 쪽의 트랜잭션과 운명을 같이하면 <b>되돌리려던 것이 함께 롤백된다.</b>
     */

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean releaseClaim(Long outboxId, String claimToken) {
        return jdbcTemplate.update("""
                UPDATE notification_outbox
                   SET status='PENDING', next_attempt_at=CURRENT_TIMESTAMP(6),
                       claimed_at=NULL, claim_token=NULL
                 WHERE id=? AND status='IN_PROGRESS' AND claim_token=?
                """, outboxId, claimToken) == 1;
    }

    /**
     * <b>상태마다 따로 세서 더한다 — {@code IN} 하나로 묶지 않는다.</b>
     *
     * <p>실측이 그 이유다. 같은 인덱스를 쓰는데 접근 방식이 다르다:
     *
     * <pre>
     *   WHERE status IN ('PENDING','IN_PROGRESS')  →  type=index  (인덱스 **전체** 스캔)
     *   WHERE status = 'PENDING'                   →  type=ref    (그 값 구간만)
     * </pre>
     *
     * <p>{@code type=index} 는 커버링이라 표는 안 읽지만 <b>인덱스는 끝까지 훑는다</b> —
     * {@code PUBLISHED}·{@code DEAD} 가 쌓일수록 비용이 는다. 15초마다 도는 질의가
     * 누적 행 수에 비례해 비싸지면 <b>관측이 사고를 키운다.</b> 리뷰가 짚었고,
     * 처음에 나는 {@code Using index} 만 보고 "인덱스만으로 센다" 로 읽었다 —
     * <b>커버링인 것과 구간만 읽는 것은 다르다.</b>
     *
     * <p>{@code ref} 두 번이 {@code index} 한 번보다 싸다. 두 값은 서로 겹치지 않는
     * 상태라 더하면 같은 수다.
     */
    @Override
    public long countBacklog() {
        return countByStatus("PENDING") + countByStatus("IN_PROGRESS");
    }

    private long countByStatus(String status) {
        Long count = jdbcTemplate.queryForObject(COUNT_BY_STATUS, Long.class, status);
        return count == null ? 0L : count;
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

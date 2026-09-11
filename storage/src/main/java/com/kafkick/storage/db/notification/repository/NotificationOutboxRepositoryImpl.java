package com.kafkick.storage.db.notification.repository;

import java.time.Instant;
import java.time.Duration;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
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
     * 백로그 질의.
     *
     * <p><b>패키지 가시성이다</b> — {@code BacklogPlanContractTest} 가 <b>이 문자열 그대로</b>
     * 실행계획을 잰다. 테스트가 자기 SQL 을 적어 두면 구현을 바꿔도 <b>테스트는 그대로
     * 통과한다</b>(실제로 그랬다).
     */
    static final String COUNT_BACKLOG =
            "SELECT COUNT(*) FROM notification_outbox WHERE status IN ('PENDING','IN_PROGRESS')";

    /**
     * 한 종류의 due 명령을 몫만큼 잠근다.
     *
     * <p><b>패키지 가시성인 이유는 {@link #COUNT_BACKLOG} 와 같다</b> —
     * {@code OutboxKindPlanContractTest} 가 <b>이 문자열 그대로</b> 실행계획을 잰다.
     *
     * <p>{@code `trigger`} 가 <b>맨 앞 술어</b>인 것이 인덱스 선택의 전부다.
     * {@code ix_notification_outbox_kind (`trigger`, status, next_attempt_at, id)} 를
     * 타야 due 백로그를 안 훑는다 — 안 타면 <b>백로그 크기에 비례해</b> 읽는다
     * (실측 5,003 vs 2).
     */
    static final String SELECT_DUE_BY_KIND = """
            SELECT id, next_attempt_at FROM notification_outbox
             WHERE `trigger`=? AND status='PENDING'
               AND next_attempt_at <= CURRENT_TIMESTAMP(6)
             ORDER BY next_attempt_at, id
             LIMIT ?
               FOR UPDATE SKIP LOCKED
            """;

    /**
     * 같은 종류를 <b>이어서</b> 잠근다 — 상대가 몫을 못 채워 자리가 남았을 때.
     *
     * <p><b>커서로 이어받지 않으면 이미 잠근 것을 다시 집는다.</b>
     * {@code SKIP LOCKED} 는 <b>남이</b> 잠근 행만 건너뛴다 — 같은 트랜잭션이 방금
     * 잠근 행은 그대로 보이므로, 커서 없이 한 번 더 부르면 앞 회차와 똑같은 앞머리가
     * 나온다.
     *
     * <p>{@code (next_attempt_at, id)} 튜플 비교인 이유는 정렬 키가 그 둘이기 때문이다.
     * {@code id} 만으로 이으면 <b>정렬 순서와 다른 축</b>이라 중간을 건너뛰거나 겹친다.
     */
    static final String SELECT_DUE_BY_KIND_AFTER = """
            SELECT id, next_attempt_at FROM notification_outbox
             WHERE `trigger`=? AND status='PENDING'
               AND next_attempt_at <= CURRENT_TIMESTAMP(6)
               AND (next_attempt_at, id) > (?, ?)
             ORDER BY next_attempt_at, id
             LIMIT ?
               FOR UPDATE SKIP LOCKED
            """;

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

    /** {@link #rotated()} 가 돌리는 회차 번호. 정확성이 아니라 공평함에만 쓰인다. */
    private final AtomicLong claimRound = new AtomicLong();

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
        // 선점이 종류를 둘로 갈라 몫을 뗀다. 셋이 되는 날 여기서 기동이 실패해야
        // 원인이 한 줄로 드러난다 — 선점 경로에서 터지면 릴레이가 산 채로 멈춘다.
        List<AttemptTrigger> kinds = AttemptTrigger.outboxKinds();
        if (kinds.size() != 2) {
            throw new IllegalStateException(
                    "선점은 종류가 둘일 때만 이 방식으로 몫을 나눕니다. 현재=" + kinds);
        }
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
            List<NotificationOutboxClaim> claims = requiresNew.execute(ignored -> claimPending(max));
            countClaimed(claims);
            return claims;
        } catch (PessimisticLockingFailureException contention) {
            // SKIP LOCKED 가 대부분을 막지만 회수 경로는 여전히 기다린다. 이번 회차를 접는다.
            return List.of();
        }
    }

    /**
     * <b>종류마다 몫을 떼어 잠근다 — 먼저 잠그고, 잠근 것만 선점 표시한다.</b>
     *
     * <h2>왜 한 줄로 안 집나</h2>
     *
     * <p>한때 여기는 {@code ORDER BY next_attempt_at, id} 하나로 due 를 훑었다. 그러면
     * <b>운영자가 방금 누른 재발송이 큐 맨 뒤에 선다</b> — {@code MANUAL} 행은
     * {@code next_attempt_at = now} 라 이미 밀린 자동 건 <b>전부보다</b> 늦기 때문이다.
     * 실측 — 한 회차에 64건({@code claimBatchSize})씩 뺄 때 {@code MANUAL} 1건이
     * 잡히기까지:
     *
     * <pre>
     *   due INITIAL     64      2 회차
     *   due INITIAL    640     11 회차
     *   due INITIAL  5,000     79 회차
     * </pre>
     *
     * <p>⌈(N+1)/64⌉ 다 — {@code MANUAL} 은 줄의 <b>N+1 번째</b>다. (한때 ⌈N/64⌉ 로
     * 적었는데, 셋 중 5,000 에서만 값이 같아 그 한 점을 보고 단정한 것이었다.)
     * 사람이 누르는 기능인 만큼 그 대기가 곧 화면의 침묵이다.
     *
     * <h2>우선순위가 아니라 몫이다</h2>
     *
     * <p>⚠️ <b>{@code MANUAL} 을 무조건 앞세우면 안 된다.</b> 재처리가 몰리는 날 자동
     * 발송이 굶는다. 기능명세가 요구하는 것은 <i>"각각 실행 기회 보장"</i> 이지
     * 우선순위가 아니다. 그래서 <b>앞선 쪽에도 상한(절반)을 건다</b> — 그 상한이 곧
     * 뒤쪽의 몫이라, 어느 쪽이 폭주해도 반대편이 매 회차 자리를 받는다.
     *
     * <p>남는 자리는 되돌려준다. 한쪽이 비었을 때 처리량이 깎이면 <b>굶는 것을 고치려다
     * 느려지는</b> 것이라, 상대가 몫을 못 채운 만큼은 앞선 쪽이 이어서 가져간다.
     *
     * <p>질의가 하나에서 <b>둘, 남는 자리를 채우면 셋</b>으로 는다. 한 문장이 읽는
     * 행은 자기 몫에 비례하고 <b>상대 종류의 적체에 안 붙는다</b> —
     * {@code OutboxKindPlanContractTest} 가 그 축을 잰다. 그것이 유지되는 것은
     * {@code ix_notification_outbox_kind} 덕이고, 그 인덱스가 왜 {@code `trigger`} 로
     * 시작하는지는 마이그레이션에 적었다.
     *
     * <p>⚠️ <b>한 회차 전체의 비용은 안 쟀다.</b> 문장별 측정을 더해서 회차 비용이라고
     * 적지 말 것 — 실제로 나가는 LIMIT 조합은 몫과 남은 자리에 따라 매번 다르다.
     *
     * <h2>잠그는 방식</h2>
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
        AttemptTrigger head = rotated();
        AttemptTrigger tail = other(head);

        // 앞선 쪽도 절반까지만 — 이 상한이 뒤쪽의 몫이다. max=1 이면 나눌 자리가 없어
        // 1 이 되는데, 그때는 회차마다 뒤바뀌는 rotated() 가 대신 번갈아 준다.
        // (claimBatch 가 max >= 1 을 이미 막으므로 이 값은 max 를 넘지 않는다.)
        int reserve = Math.max(1, max / 2);

        List<DueRow> claimed = new ArrayList<>(max);
        claimed.addAll(lockDue(head, reserve, null));
        int headCount = claimed.size();
        claimed.addAll(lockDue(tail, max - headCount, null));

        // 상대가 몫을 못 채워 남은 자리는 앞선 쪽이 마저 쓴다. 한쪽이 비었다고
        // 처리량을 깎지 않기 위해서다.
        //
        // 앞선 쪽이 자기 상한을 꽉 채웠을 때만 이어 붙인다. 못 채웠다는 것은
        // **그 순간 집을 수 있는 것을 다 집었다**는 뜻이라 한 번 더 물어봐야 헛돈다 —
        // SKIP LOCKED 는 잠긴 행에서 멈추지 않고 LIMIT 을 채울 때까지 훑는다(실측:
        // 표 20행 중 앞 10건이 잠긴 상태에서 LIMIT 5 가 11~15번째 5건을 돌려준다).
        // 그래서 미달은 고갈이거나 남은 것이 전부 남의 손에 있는 상태이고, 둘 다
        // 이어 물어봐야 빈손이다.
        int spare = max - claimed.size();
        if (spare > 0 && headCount == reserve) {
            claimed.addAll(lockDue(head, spare, claimed.get(headCount - 1)));
        }

        List<Long> ids = new ArrayList<>(claimed.size());
        for (DueRow row : claimed) {
            ids.add(row.id());
        }
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

    /** 정렬 키를 통째로 들고 다닌다 — 이어받기 커서가 그 둘이라 {@code id} 만으로는 부족하다. */
    private record DueRow(long id, Timestamp nextAttemptAt) { }

    /**
     * 한 종류의 due 명령을 {@code limit} 건까지 잠근다.
     *
     * <p>{@code after} 가 있으면 그 뒤부터 이어받는다({@link #SELECT_DUE_BY_KIND_AFTER}).
     *
     * <p>{@code limit} 이 0 이면 <b>질의를 안 보낸다.</b> {@code LIMIT 0} 은 오류 없이
     * 빈 결과를 주지만 왕복은 그대로 쓴다 — 100ms 주기로 도는 경로다.
     */
    private List<DueRow> lockDue(AttemptTrigger kind, int limit, DueRow after) {
        if (limit <= 0) {
            return List.of();
        }
        RowMapper<DueRow> mapper =
                (rs, row) -> new DueRow(rs.getLong("id"), rs.getTimestamp("next_attempt_at"));
        return after == null
                ? jdbcTemplate.query(SELECT_DUE_BY_KIND, mapper, kind.name(), limit)
                : jdbcTemplate.query(SELECT_DUE_BY_KIND_AFTER, mapper,
                        kind.name(), after.nextAttemptAt(), after.id(), limit);
    }

    /**
     * 이번 회차에 <b>먼저</b> 몫을 떼어 갈 종류. 회차마다 번갈아 돈다.
     *
     * <p><b>{@code max=1} 때문에 있다.</b> 백프레셔가 배치를 1 로 자르면 한 자리를
     * 나눌 수가 없어 몫이 무의미해진다 — 순서가 고정이면 그 회차들 동안 뒤쪽은
     * <b>영영</b> 안 잡힌다. 번갈아 돌면 그 경우에도 둘 다 기회를 받는다.
     *
     * <p>{@code max >= 2} 에서는 이 회전이 <b>정확성에 필요하지 않다</b> — 몫이 이미
     * 양쪽을 보장한다. 순서만 흔들 뿐이다.
     *
     * <p>프로세스가 여럿이면 각자 돈다. 맞춰야 할 이유가 없다: 굶지 않는다는 성질은
     * 회전이 아니라 몫에서 나오고, 맞추려면 공유 상태가 하나 더 생긴다.
     */
    private AttemptTrigger rotated() {
        List<AttemptTrigger> kinds = AttemptTrigger.outboxKinds();
        return kinds.get((int) Math.floorMod(claimRound.getAndIncrement(), kinds.size()));
    }

    /**
     * 나머지 한 종류.
     *
     * <p>종류가 둘이라는 전제 위에 서 있다. 그 전제는 <b>생성자가 기동 시점에</b>
     * 확인하므로 여기서는 다시 안 본다 — 100ms 마다 도는 경로에서 컴파일 타임
     * 불변식을 검사하면, 틀렸을 때 <b>릴레이가 산 채로 아무것도 안 집으면서</b>
     * 스택트레이스만 찍는다(스케줄러가 예외를 삼키고 재스케줄한다).
     */
    private static AttemptTrigger other(AttemptTrigger kind) {
        List<AttemptTrigger> kinds = AttemptTrigger.outboxKinds();
        return kinds.get(0) == kind ? kinds.get(1) : kinds.get(0);
    }

    /**
     * 종류별로 집힌 수를 센다.
     *
     * <p><b>여기가 이미 커밋 뒤다.</b> 선점은 {@code REQUIRES_NEW} 라 부르는 쪽으로
     * 돌아온 시점에 이미 커밋돼 있다 — 그래서 {@link #afterCommit(Runnable)} 을 쓰지
     * 않는다.
     *
     * <p>⚠️ <b>한때 여기서 동기화를 걸었다가 반대로 틀렸다.</b> 이 자리에서 살아 있는
     * 동기화는 선점 트랜잭션이 아니라 <b>바깥</b> 트랜잭션의 것이다({@code REQUIRES_NEW}
     * 가 바깥을 멈춰 뒀다 되살린다). 그러면 바깥이 롤백될 때 행은 {@code IN_PROGRESS}
     * 로 커밋된 채 <b>숫자만 안 오른다</b> — 막겠다던 어긋남이 정확히 반대 방향으로 난다.
     *
     * <p><b>안 집힌 종류를 0 으로 부르지 않는다.</b> 그렇게 해도 아무 일이 안 일어나기
     * 때문이다 — 시계열을 존재하게 만드는 것은 {@link NotificationOutboxMeter} 생성자의
     * 사전 등록이고, {@code increment(0)} 은 그 위에 0 을 더할 뿐이다.
     */
    private void countClaimed(List<NotificationOutboxClaim> claims) {
        if (claims.isEmpty()) {
            return;
        }
        Map<AttemptTrigger, Integer> counts = new EnumMap<>(AttemptTrigger.class);
        for (NotificationOutboxClaim claim : claims) {
            counts.merge(claim.trigger(), 1, Integer::sum);
        }
        meter.claimed(counts);
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
        if (!updated) {
            // 0행 = claim_token 이 안 맞는다 = lease 가 만료돼 남이 가져갔다.
            // **부르는 쪽은 이미 발행했다.** 그 사실이 남는 자리가 여기뿐이다.
            afterCommit(meter::fenceLost);
        }
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
     * <b>인덱스의 두 구간만 읽는다 — 표도, 인덱스 전체도 안 훑는다.</b>
     *
     * <p>15초마다 도는 질의라 <b>누적 행 수에 비례해 비싸지면 관측이 사고를 키운다.</b>
     * 그래서 그렇지 않다는 것을 실측으로 확인했다 — {@code PUBLISHED} 300건을 심고
     * {@code EXPLAIN ANALYZE} 를 걸면:
     *
     * <pre>
     *   Covering index range scan using ix_notification_outbox_pending
     *     over (status = 'IN_PROGRESS') OR (status = 'PENDING')
     *     (actual rows=5)
     * </pre>
     *
     * <p>읽은 행이 <b>백로그 크기(5)</b>이지 표 크기(305)가 아니다.
     *
     * <p>⚠️ <b>한때 이것을 상태별 질의 둘로 쪼갰다.</b> {@code EXPLAIN} 의
     * {@code type=index} 를 <i>"인덱스를 끝까지 훑는다"</i> 로 읽었기 때문인데,
     * <b>접근 방식 이름만 보고 비용을 단정한 것이었다</b> — 리뷰가 그 단정이 입증되지
     * 않았다고 짚었고, 재 보니 실제로는 두 구간만 읽는다. 쪼개면 질의가 둘이라
     * <b>그 사이에 행이 옮겨 가</b> 두 번 세이거나 안 세이므로, 되돌리는 것이 단순하면서
     * 더 정확하다 — 한 문장이면 스냅샷 걱정 자체가 없다.
     */
    @Override
    public long countBacklog() {
        Long count = jdbcTemplate.queryForObject(COUNT_BACKLOG, Long.class);
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

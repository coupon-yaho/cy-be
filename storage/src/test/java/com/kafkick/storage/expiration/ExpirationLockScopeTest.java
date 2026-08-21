// 만료 SQL 이 무엇을 잠그는지 확인합니다. 무엇을 넘겼는지는 ExpirationJdbcAdapterTest 가 봅니다.
package com.kafkick.storage.expiration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.function.IntSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import javax.sql.DataSource;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>만료 배치가 발급 경로를 막지 않는지 지킨다.</b> 예전에는 막았고, 그 비용을 수치로 못 박아
 * 두었다가 인덱스와 격리 수준을 도입해 풀었다. 이제 이 클래스는 <b>풀린 상태를 지키는</b> 쪽이다.
 *
 * <p><b>무엇이 문제였나.</b> {@code status}·{@code expires_at} 에 인덱스가 없어 첫 문장이 PK 를
 * 끝까지 훑었고, 넘길 것이 없는 실행이 최악이었다 — {@code LIMIT} 이 발동할 일이 없어서다.
 * 5분 주기라 하루 288회 중 대부분이 그 경로이고, 대상이 많은 날도 마지막 청크는 반드시 그렇다.
 *
 * <p><b>무엇으로 풀었나.</b> 둘이 서로 다른 문제를 푼다.
 *
 * <pre>
 *   만료 대상 0건 · 5,000행 기준
 *
 *                          락      발급 INSERT   사용 UPDATE
 *   인덱스 없음 · RR      5,020        1205         1205
 *   인덱스 있음 · RR          2        1205         통과      ← 스캔 비용만 풀린다
 *   인덱스 있음 · RC          0        통과         통과      ← 잠금 위치까지 풀린다
 * </pre>
 *
 * <p>인덱스가 락 수를 줄이고({@code V11}), READ COMMITTED 가 gap 락을 없앤다
 * ({@code ExpireJobConfig}). 인덱스만으로는 발급이 계속 막히는데, 막는 것이 스캔 범위가 아니라
 * <b>보조 인덱스의 gap</b> 이기 때문이다. 전체 수치와 재는 방법은
 * {@code docs/12-expire-lock-measurement.md} 에 있다.
 *
 * <p><b>그래서 아래 단언은 상한이다.</b> 넘긴 건수에 비례하는 만큼만 잠가야 하고, 그보다 커지면
 * 둘 중 하나가 풀린 것이다 — 인덱스가 사라졌거나 격리 수준이 되돌아갔거나.
 *
 * <p><b>클래스 트랜잭션을 끈다.</b> {@code @DataJpaTest} 가 테스트를 트랜잭션으로 감싸면
 * <b>시드 INSERT 가 잡은 락과 어댑터가 잡은 락이 한 트랜잭션에 섞인다</b> — 200행을 심은 것만으로
 * 402개가 잡혀서 무엇을 재는지 알 수 없게 된다. 그래서 시드는 자동 커밋으로 넣고,
 * 어댑터 호출만 {@link TransactionTemplate} 으로 감싸 그 안에서 센다.
 *
 * <p>대신 심은 데이터가 커밋되어 남으므로 {@link #tearDown()} 이 직접 지운다.
 */
@RepositoryTest
@Import(ExpirationJdbcAdapter.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class ExpirationLockScopeTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final LocalDateTime EXPIRED_AT = AS_OF.minusDays(1);
    private static final LocalDateTime ALIVE_AT = AS_OF.plusDays(1);
    private static final LocalDateTime WROTE_AT = AS_OF.plusMinutes(3);
    private static final int NO_LIMIT = 1000;

    /** 회차 하나가 담을 수 있는 재고. 넘기면 {@code ck_stock_range} 가 거부한다. */
    private static final int STOCK_PER_COUPON = 100;

    @Autowired
    private ExpirationJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private VerificationSeed seed;
    private TransactionTemplate transaction;

    @BeforeEach
    void setUp() {
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
        transaction = new TransactionTemplate(transactionManager);
        // 실제 잡과 같은 격리에서 재야 의미가 있다. ExpireJobConfig 가 만료 Step 에
        // READ COMMITTED 를 걸어 두는데, 여기서 기본값(REPEATABLE READ)으로 재면
        // gap 락이 그대로 잡혀 운영에서 일어나지 않는 상황을 재게 된다.
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    @AfterEach
    void tearDown() {
        seed.clear();
    }

    /** 기한을 직접 세운다. 시드는 발급 시각만 받아서 만료 시각을 따로 정할 수단이 없다. */
    private void issuance(IssuanceStatus status, LocalDateTime expiresAt) {
        long id = seed.issuance(status);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", expiresAt)
                .param("id", id)
                .update();
    }

    /**
     * {@code ISSUED} 는 회차 재고를 올리므로 상한에 걸리지 않게 나눠 담는다.
     * 인덱스는 회차와 무관하니 회차를 몇 개로 쪼개든 락 범위 관측에는 영향이 없다.
     */
    private void issuances(int count, IssuanceStatus status, LocalDateTime expiresAt) {
        for (int i = 0; i < count; i++) {
            if (i % (STOCK_PER_COUPON / 2) == 0) {
                seed.newCoupon();
            }
            issuance(status, expiresAt);
        }
    }

    /**
     * <b>지금 트랜잭션이 {@code issuances} 에 잡고 있는 레코드 락 수.</b>
     * {@code PS_CURRENT_THREAD_ID()} 로 우리 커넥션 것만 센다 — 테이블 이름만으로 거르면
     * 옆에서 도는 테스트의 락까지 딸려 온다.
     *
     * <p>읽으려면 {@code PROCESS} 와 {@code performance_schema} 의 {@code SELECT} 가 둘 다
     * 필요하다. 테스트 계정에 주는 자리는 {@code db/testcontainers/grant-process.sql} 이다.
     */
    private int lockedRecords() {
        return jdbcClient.sql("""
                        SELECT COUNT(*)
                          FROM performance_schema.data_locks
                         WHERE OBJECT_NAME = 'issuances'
                           AND LOCK_TYPE = 'RECORD'
                           AND THREAD_ID = PS_CURRENT_THREAD_ID()
                        """)
                .query(Integer.class)
                .single();
    }

    /** 어댑터를 트랜잭션으로 감싸고, 커밋으로 락이 풀리기 전에 센다. */
    private int locksTakenBy(IntSupplier call, int expectedRows) {
        return transaction.execute(status -> {
            assertThat(call.getAsInt())
                    .as("넘긴 건수가 먼저 맞아야 락 수치가 뜻을 갖는다")
                    .isEqualTo(expectedRows);
            return lockedRecords();
        });
    }

    /**
     * <b>넘길 것이 하나도 없는 실행이 최악이었다.</b> {@code LIMIT} 이 발동할 일이 없어 끝까지
     * 훑었고, 하나도 안 바꾸면서 지나간 행을 전부 잠갔다. 지금은 인덱스가 그 스캔을 없앤다.
     */
    @Test
    @DisplayName("넘길 것이 없는 실행은 훑지도 잠그지도 않는다")
    void locksNothingWhenNothingExpires() {
        int rows = 200;
        issuances(rows, IssuanceStatus.ISSUED, ALIVE_AT);

        int locked = locksTakenBy(() -> adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT), 0);

        assertThat(locked)
                .as("한 건도 안 넘겼으면 잠글 것도 없다. %d 에 가까워지면 인덱스가 사라진 것이다", rows)
                .isLessThan(20);
    }

    /**
     * <b>진도({@code afterId})는 한 번의 잡 실행 안에서만 산다.</b> 다음 실행은 다시
     * {@code id > 0} 부터 시작하므로 이미 처리가 끝난 앞 구간을 매번 새로 지나간다.
     * 예전에는 지나가면서 전부 잠갔다 — 넘길 것이 10건뿐이어도 비용이 테이블 크기를 따라갔다.
     * 인덱스가 그 구간을 아예 안 보게 만든다.
     */
    @Test
    @DisplayName("이미 처리가 끝난 앞 구간은 잠그지 않는다 — 넘긴 건수에만 비례한다")
    void locksOnlyWhatItExpires() {
        int settled = 190;
        int toExpire = 10;
        issuances(settled, IssuanceStatus.EXPIRED, EXPIRED_AT);
        issuances(toExpire, IssuanceStatus.ISSUED, EXPIRED_AT);

        int locked = locksTakenBy(() -> adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT), toExpire);

        assertThat(locked)
                .as("%d 건만 넘겼다. 이미 끝난 %d 건까지 잠그면 앞 구간을 다시 훑은 것이다",
                        toExpire, settled)
                .isLessThan(settled / 2);
    }

    /**
     * <b>청크 전체가 잡는 락을 잰다.</b> 첫 문장만 재면 뒤 문장이 뻗는 공유 락이 기록에서 빠지고,
     * 그때 막히는 것은 테스트가 아니라 운영의 발급 경로다.
     *
     * <p>상한({@code id <= lastId})이 그 범위를 청크로 닫는다. 그것이 빠지면 이 단언이
     * 행 수에 가까운 값을 보게 된다 — 인덱스 유무와 무관하게 상한만으로 지켜지는 성질이다.
     *
     * <p><b>만료 대상을 앞쪽에 심는 것이 이 측정의 조건이다.</b> 뒤쪽에 심으면
     * {@code expireBatch} 가 {@code LIMIT} 을 못 채워 끝까지 훑고, 그 X 락이 테이블 전체를
     * 덮어 <b>뒤 문장의 증가분을 가린다</b> — 재려는 것이 그 증가분인데 측정이 성립하지 않는다.
     *
     * <p>앞쪽 {@code toExpire} 건에서 {@code LIMIT} 이 차면 {@code expireBatch} 가 거기서
     * 멈추므로, 그 뒤에 늘어나는 락은 전부 나머지 다섯 문장이 잡은 것이다.
     */
    @Test
    @DisplayName("첫 문장이 멈춘 뒤에도 나머지 문장이 그 구간 밖을 잠그지 않는다")
    void locksStayWithinTheChunkAcrossAllStatements() {
        int toExpire = 20;
        int beyond = 400;
        issuances(toExpire, IssuanceStatus.ISSUED, EXPIRED_AT);
        issuances(beyond, IssuanceStatus.EXPIRED, EXPIRED_AT);

        int[] measured = transaction.execute(status -> {
            assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, toExpire)).isEqualTo(toExpire);
            int afterFirst = lockedRecords();

            long lastId = adapter.lastExpiredId(AS_OF, WROTE_AT, 0L);
            assertThat(lastId).as("경계를 못 구하면 뒤 문장이 전부 빈 집합을 본다").isPositive();

            // lastExpiredId 는 상한을 못 가지므로(그것을 구하는 문장이다) 스캔 축 밖에 둔다.
            long before = rowsRead();
            adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, lastId);
            adapter.expiredCouponCount(AS_OF, WROTE_AT, 0L, lastId);
            adapter.stockRowCount(AS_OF, WROTE_AT, 0L, lastId);
            adapter.releaseStock(AS_OF, WROTE_AT, 0L, lastId);
            long scanned = rowsRead() - before;

            return new int[] {afterFirst, lockedRecords(), Math.toIntExact(scanned)};
        });

        assertThat(measured[0])
                .as("LIMIT 이 앞에서 차므로 첫 문장은 그 구간만 잡는다")
                .isLessThan(toExpire * 3);
        assertThat(measured[1])
                .as("나머지 문장이 잡은 몫까지 합쳐도 구간 안이어야 한다")
                .isLessThan(toExpire * 5);
        assertThat(measured[2])
                .as("**상한을 지키는 것은 이 축이다.** RC 라 뒤 문장들이 락을 안 잡아서 "
                        + "위 두 단언은 상한을 지워도 그대로 통과한다 — 인덱스 쪽에서 겪은 것과 "
                        + "같은 구멍이다. 상한이 빠지면 심은 %d 행을 다시 훑는다", toExpire + beyond)
                .isLessThan((toExpire + beyond) / 2);
    }

    /**
     * <b>지켜야 할 명제를 직접 단언한다 — 만료가 도는 중에도 발급이 들어간다.</b>
     *
     * <p>다른 단언들은 락 <b>개수</b>를 센다. 그것은 대리 지표다 — supremum 락 하나는
     * {@code data_locks} 에서 한 줄에 불과한데 그 하나가 신규 INSERT 를 전부 막는다.
     * 즉 개수만 보면 <b>"20건만 잠갔지만 발급은 전멸"</b> 과 <b>"20건만 잠갔고 발급은 정상"</b>
     * 을 구분하지 못한다.
     *
     * <p>그리고 락 수치 단언들은 인덱스·격리 수준이 바뀌면 함께 흔들린다. 그때
     * <b>정작 지켜야 할 명제를 붙들고 있는 것이 없어진다.</b> 이 테스트가 그 자리를 맡는다.
     *
     * <p><b>별도 커넥션이어야 한다.</b> 같은 커넥션이면 자기 락에는 안 걸려 무엇도 증명하지 못한다.
     * 대기는 2초로 끊는다 — 막히면 그 자리에서 실패해야지, 기본값(50초)으로 늘어지면
     * 테스트가 느려질 뿐 원인은 같다.
     */
    @Test
    @DisplayName("만료 청크가 열려 있는 동안에도 신규 발급 INSERT 가 통과한다")
    void issuanceInsertPassesWhileChunkIsOpen() {
        issuances(200, IssuanceStatus.ISSUED, ALIVE_AT);
        long spareCoupon = seed.newCoupon();
        Issuer issuer = anyIssuer();

        transaction.executeWithoutResult(status -> {
            assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT))
                    .as("기한이 남았으니 한 건도 안 넘어간다 — 최악의 경로다")
                    .isZero();

            assertThatCode(() -> insertIssuanceOnSeparateConnection(spareCoupon, issuer))
                    .as("여기서 1205 가 나면 만료 배치가 도는 동안 발급이 죽는다는 뜻이다")
                    .doesNotThrowAnyException();
        });
    }

    /** 발급 한 건을 만드는 데 필요한 참조값. 시드가 만든 것을 재사용한다. */
    private record Issuer(long memberId, String grade) {
    }

    /**
     * 회차만 다르면 {@code uk_coupon_member} 에 안 걸린다. 등급은 {@code grades} 를 참조하므로
     * 아무 문자열이나 넣으면 FK 로 막힌다 — 그러면 <b>락과 무관한 이유로 테스트가 실패</b>해서
     * 무엇을 재는지 알 수 없게 된다.
     */
    private Issuer anyIssuer() {
        return jdbcClient.sql("SELECT member_id, issued_grade FROM issuances ORDER BY id LIMIT 1")
                .query((rs, rowNum) -> new Issuer(rs.getLong(1), rs.getString(2)))
                .single();
    }

    /**
     * 발급 경로가 할 일을 그대로 쓴다 — {@code issuances} 에 행을 하나 넣는다.
     *
     * <p>{@link DataSource} 에서 커넥션을 직접 얻는다. {@code JdbcClient} 를 쓰면 진행 중인
     * 트랜잭션의 커넥션에 붙어 자기 락을 만나지 않는다.
     */
    private void insertIssuanceOnSeparateConnection(long couponId, Issuer issuer) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement timeout = connection.createStatement()) {
                timeout.execute("SET SESSION innodb_lock_wait_timeout = 2");
            }
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO issuances
                           (coupon_id, member_id, code, issued_grade, status,
                            issued_at, expires_at, updated_at)
                    VALUES (?, ?, 'LOCKSCOPE0000001', ?, 'ISSUED', ?, ?, ?)
                    """)) {
                insert.setLong(1, couponId);
                insert.setLong(2, issuer.memberId());
                insert.setString(3, issuer.grade());
                insert.setTimestamp(4, Timestamp.valueOf(AS_OF));
                insert.setTimestamp(5, Timestamp.valueOf(ALIVE_AT));
                insert.setTimestamp(6, Timestamp.valueOf(AS_OF));
                insert.executeUpdate();
            }
        }
    }

    /**
     * <b>인덱스가 지키는 것은 락이 아니라 스캔이다.</b>
     *
     * <p>READ COMMITTED 로 내린 뒤로는 매치되지 않은 행의 락을 즉시 놓으므로,
     * <b>인덱스가 없어도 락 수는 0 이다.</b> 그래서 이 클래스의 다른 단언들로는 인덱스가
     * 사라진 것을 못 잡는다 — 실제로 {@code V11} 을 지우는 돌연변이가 락 테스트를
     * 전부 통과했다.
     *
     * <p>남는 차이는 <b>읽은 행 수</b>다. 실측(5,000행·만료 대상 0건·RC):
     * 인덱스 없음 <b>5,017행</b> → 인덱스 있음 <b>1행</b>. 운영 300만 행이면 매 실행이
     * 300만 행을 읽느냐 마느냐의 차이이고, 그것이 5분 주기를 지킬 수 있느냐를 가른다.
     *
     * <p>{@code Handler_read_*} 는 스토리지 엔진이 실제로 넘긴 행을 센다 —
     * {@code EXPLAIN} 의 추정치가 아니라 실행 결과다.
     */
    @Test
    @DisplayName("만료 대상이 없으면 행을 읽지도 않는다 — 인덱스가 지키는 축")
    void scansNothingWhenNothingExpires() {
        int rows = 200;
        issuances(rows, IssuanceStatus.ISSUED, ALIVE_AT);

        long scanned = transaction.execute(status -> {
            // FLUSH STATUS 는 PreparedStatement 로 못 돌고 RELOAD 권한도 필요하다.
            // 전후 차이로 재면 둘 다 피한다 — 재는 질의 자신이 올리는 몫은 양쪽에 똑같이 섞인다.
            long before = rowsRead();
            assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT)).isZero();
            return rowsRead() - before;
        });

        assertThat(scanned)
                .as("인덱스가 없으면 %d 행을 전부 읽는다. 락은 RC 라 0 이어서 이 축만 남는다", rows)
                .isLessThan(rows / 4);
    }

    /**
     * <b>경계를 구하는 문장은 상한을 가질 수 없다 — 그 상한을 구하는 것이 이 문장이다.</b>
     *
     * <p>여섯 문장 중 다섯은 {@code (afterId, lastId]} 로 닫혀 있어
     * {@link #locksStayWithinTheChunkAcrossAllStatements()} 의 스캔 축이 지킨다.
     * {@code lastExpiredId} 만 그 축 밖에 있고, 그래서 여기서 따로 잰다.
     *
     * <pre>
     *   SELECT COALESCE(MAX(id), :afterId) FROM issuances
     *    WHERE status = 'EXPIRED' AND updated_at = :committedAt
     *      AND expires_at &lt; :asOf AND id &gt; :afterId
     * </pre>
     *
     * <p>{@code V11(status, expires_at)} 은 이 문장을 <b>안 돕는다.</b> 옵티마이저가 그것을
     * 고르면 <b>EXPIRED 이면서 기한이 지난 행 전부</b>를 범위로 잡고 각 행에서
     * {@code updated_at} 을 확인한다. 좁히는 것은 {@code V12(updated_at, id)} 다.
     *
     * <p><b>이미 만료된 행이 쌓여야 드러난다.</b> 처음 잰 데이터에는 만료 대상만 있고 기존
     * {@code EXPIRED} 가 없어서 아무 문제도 안 보였다 — 그래서 이 테스트는 <b>운영 형상</b>,
     * 즉 지난 주기들이 남긴 {@code EXPIRED} 가 누적된 상태를 만든다.
     * 실측(200,000행·누적 150,000·대상 5,000): 청크1 <b>200,017행</b> → <b>1,001행</b>.
     *
     * <p><b>매 실행의 첫 청크가 이 비용을 낸다.</b> 진도는 실행 사이로 안 넘어가므로
     * 언제나 {@code afterId = 0} 부터 시작한다 — 하루 288회다.
     */
    @Test
    @DisplayName("경계를 구하는 문장이 누적된 EXPIRED 를 다시 훑지 않는다 — V12 가 지키는 축")
    void boundaryStatementDoesNotRescanAccumulatedExpirations() {
        int settled = 300;
        int toExpire = 10;
        issuances(settled, IssuanceStatus.EXPIRED, EXPIRED_AT);
        // 지난 주기들이 남긴 것이다. 이번 표식(WROTE_AT)과 갈라야 운영 형상이 된다.
        jdbcClient.sql("UPDATE issuances SET updated_at = :at WHERE status = 'EXPIRED'")
                .param("at", AS_OF.minusDays(2))
                .update();
        issuances(toExpire, IssuanceStatus.ISSUED, EXPIRED_AT);

        long scanned = transaction.execute(status -> {
            assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, NO_LIMIT)).isEqualTo(toExpire);

            long before = rowsRead();
            long lastId = adapter.lastExpiredId(AS_OF, WROTE_AT, 0L);
            long read = rowsRead() - before;

            assertThat(lastId).as("경계를 못 구하면 뒤 문장이 전부 빈 집합을 본다").isPositive();
            return read;
        });

        assertThat(scanned)
                .as("이번에 넘긴 %d 건만 보면 된다. 누적된 %d 건까지 읽으면 비용이 테이블 크기를 "
                        + "따라간다 — 5분 주기를 지켜야 하는 잡에서 매 실행 첫 청크마다 난다",
                        toExpire, settled)
                .isLessThan(settled / 3);
    }

    /** 스토리지 엔진이 실제로 넘긴 행 수. 추정치가 아니라 실행 결과다. */
    private long rowsRead() {
        return jdbcClient.sql("""
                        SELECT COALESCE(SUM(VARIABLE_VALUE), 0)
                          FROM performance_schema.session_status
                         WHERE VARIABLE_NAME IN
                               ('Handler_read_next', 'Handler_read_rnd_next', 'Handler_read_first')
                        """)
                .query(Long.class)
                .single();
    }
}

// 회차 전이가 대상 밖 행을 잠그지 않는지 실제 락으로 확인합니다.
package com.kafkick.storage.db.coupon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.storage.db.RepositoryTest;
import com.kafkick.storage.db.coupon.repository.CouponRoundJdbcAdapter;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>이 테스트가 막는 것은 성능이 아니라 회귀다.</b> 회차 전이는 <i>"대상을 고른 뒤 id
 * 하나씩"</i> 인데, 다음 사람이 그것을 비효율로 보고 집합 {@code UPDATE} 한 문장으로 합치면
 * <b>기능 테스트 20개가 전부 초록이다</b> — 상태 결과가 같기 때문이다. 그 순간 1분마다
 * 발급 경로가 죽는다.
 *
 * <p><b>실측(MySQL 8.0, 회차 150행 · {@code SCHEDULED} 10).</b> {@code coupons} 에는 그 조건을
 * 받칠 인덱스가 없어 집합 {@code UPDATE} 가 테이블을 통째로 훑고({@code type=index rows=150}),
 * 스캔한 레코드 전부와 갭에 X 락을 잡는다 — {@code data_locks} 가 <b>151</b>(전부 + supremum)
 * 이었고, 그 트랜잭션이 열린 동안 다른 세션의 <b>재고 소진 {@code CLOSED} UPDATE 와 발급 전
 * {@code FOR SHARE} 가 둘 다 {@code ERROR 1205}</b> 였다. id 단건은
 * {@code X,REC_NOT_GAP} 10 만 잡고 셋 다 통과했다.
 *
 * <p>⚠️ <b>아래 락 테스트들은 "집합 UPDATE 로의 회귀" 를 못 잡는다.</b> RC 에서는 집합
 * {@code UPDATE} 도 발급을 안 막아서, 그 돌연변이를 넣었을 때 셋이 전부 초록이었다.
 * 그 축을 지키는 것은 {@link #transitionsRunAtReadCommitted()} 와, 재고 {@code EXISTS} 조건을
 * 재는 스케줄러 테스트들이다. 여기 셋이 지키는 것은 <b>결과 명제</b>다 —
 * <i>"전이가 도는 동안 발급 경로가 죽지 않는다"</i>.
 */
@RepositoryTest
// @DataJpaTest 슬라이스는 @Repository 를 안 스캔한다 — 형제 어댑터 테스트들이 같은 이유로
// 같은 모양이다. 시간 손잡이는 기본값(5초)을 쓴다.
@Import(CouponRoundJdbcAdapter.class)
// **테스트 트랜잭션을 안 쓴다.** @DataJpaTest 는 테스트마다 트랜잭션을 열고 롤백하는데,
// 그러면 시드 INSERT 가 커밋되지 않아 **그 락이 측정에 섞인다** — 별도 커넥션이 남의 회차를
// 만지려 할 때 우리 시드의 미커밋 INSERT 에 걸려 1205 가 난다(실제로 그렇게 빨개졌다).
// 시드를 자동 커밋으로 넣고, 커밋되어 남는 것은 @AfterEach 가 지운다.
// ExpirationLockScopeTest 가 같은 이유로 같은 모양이다.
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class CouponRoundLockScopeTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 4, 12, 5, 0);

    @Autowired
    private CouponRoundRepository rounds;

    /** 격리수준 단언이 어댑터의 실물 필드를 읽는다 — 포트에는 그 축이 없다. */
    @Autowired
    private CouponRoundJdbcAdapter adapter;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private DataSource dataSource;

    private VerificationSeed seed;

    @BeforeEach
    void resetRounds() {
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    /** 커밋해서 남긴 것을 직접 지운다 — 트랜잭션 롤백이 없으니 다음 테스트가 그것을 본다. */
    @AfterEach
    void tearDown() {
        seed.clear();
    }

    /**
     * <b>대상 밖 회차의 행을 안 잠근다.</b> 집합 {@code UPDATE} 로 되돌아가면 이 수가
     * 회차 수 + supremum 으로 뛴다.
     *
     * <p>어댑터가 전이마다 자기 트랜잭션을 열고 커밋하므로, 호출이 끝난 뒤에는 락이 없다 —
     * 그래서 <b>다른 세션이 남의 회차 행을 쥔 상태</b>에서 전이가 통과하는지로 잰다.
     * 그것이 이 설계가 실제로 지키려는 명제다.
     */
    @Test
    @DisplayName("다른 세션이 남의 회차 행을 쥐고 있어도 전이가 통과한다 — 집합 UPDATE 면 1205 다")
    void transitionPassesWhileAnotherRoundIsLocked() throws Exception {
        long target = seed.round(CouponStatus.SCHEDULED.name(), NOW.minusMinutes(1), NOW.plusDays(1));
        long other = seed.round(CouponStatus.OPEN.name(), NOW.minusDays(1), NOW.plusDays(1));

        try (Connection held = dataSource.getConnection()) {
            held.setAutoCommit(false);
            // 발급 경로가 그 회차의 상태를 확인하는 모양. 이 락은 target 과 무관하다.
            try (PreparedStatement lock = held.prepareStatement(
                    "SELECT status FROM coupons WHERE id = ? FOR SHARE")) {
                lock.setLong(1, other);
                lock.executeQuery().close();
            }

            assertThatCode(() -> rounds.open(target, NOW))
                    .as("여기서 1205 가 나면 전이가 대상 밖 행까지 잠근다는 뜻이다 — "
                            + "집합 UPDATE 로 되돌아간 상태가 정확히 그렇다")
                    .doesNotThrowAnyException();
            assertThat(statusOf(target)).isEqualTo(CouponStatus.OPEN.name());

            held.rollback();
        }
    }

    /**
     * <b>반대 방향도 잰다.</b> 전이가 쥔 락 때문에 <b>발급 경로가</b> 막히면 안 된다.
     * 어댑터의 트랜잭션은 문장 하나짜리라 이 테스트가 잡는 것은 <i>"그 문장이 남의 행을
     * 건드리지 않는다"</i> 이고, 그것을 <b>전이 도중에</b> 재려면 별도 커넥션이 전이 대상이
     * 아닌 회차를 동시에 갱신하는 모양이어야 한다.
     */
    @Test
    @DisplayName("전이 중에도 다른 회차의 재고 소진 CLOSED 가 통과한다")
    void soldOutClosePassesWhileTransitioning() throws Exception {
        long target = seed.round(CouponStatus.SCHEDULED.name(), NOW.minusMinutes(1), NOW.plusDays(1));
        long soldOut = seed.round(CouponStatus.OPEN.name(), NOW.minusDays(1), NOW.plusDays(1));

        assertThat(rounds.open(target, NOW)).isTrue();

        try (Connection issuance = dataSource.getConnection();
                Statement statement = issuance.createStatement()) {
            statement.execute("SET SESSION innodb_lock_wait_timeout = 2");
            assertThatCode(() -> statement.executeUpdate(
                    "UPDATE coupons SET status = 'CLOSED' WHERE id = " + soldOut
                            + " AND status = 'OPEN'"))
                    .as("발급 경로의 재고 소진 마감이 전이 때문에 막히면 안 된다")
                    .doesNotThrowAnyException();
        }
    }

    /**
     * <b>전이 한 건이 잡는 락이 그 회차 하나뿐인지 센다.</b> 어댑터가 커밋해 버리므로
     * 호출 뒤에는 셀 수 없다 — 그래서 같은 문장을 이 테스트의 커넥션에서 직접 실행해
     * 커밋 전에 센다. 어댑터가 쓰는 SQL 모양과 <b>같은 모양</b>이어야 뜻이 있다.
     */
    @Test
    @DisplayName("id 단건 UPDATE 는 그 회차 행만 잠근다 — 집합 UPDATE 면 회차 수 + supremum 이다")
    void singleRowUpdateLocksOnlyThatRow() throws Exception {
        for (int i = 0; i < 5; i++) {
            seed.round(CouponStatus.SCHEDULED.name(),
                    NOW.minusMinutes(1), NOW.plusDays(1));
        }
        long target = seed.round(CouponStatus.SCHEDULED.name(), NOW.minusMinutes(1), NOW.plusDays(1));

        try (Connection held = dataSource.getConnection()) {
            held.setAutoCommit(false);
            try (PreparedStatement update = held.prepareStatement(
                    "UPDATE coupons SET status = 'OPEN' WHERE id = ? AND status = 'SCHEDULED' "
                            + "AND close_at > ?")) {
                update.setLong(1, target);
                update.setObject(2, NOW);
                assertThat(update.executeUpdate()).isOne();
            }

            assertThat(lockedCouponRecords(held))
                    .as("대상 하나만 잠가야 한다. 집합 UPDATE 로 되돌아가면 회차 수 + "
                            + "supremum 으로 뛴다")
                    .isOne();

            held.rollback();
        }
    }

    /**
     * <b>이 테스트 파일에서 가장 중요한 단언이다.</b> 발급을 살리는 것은 id 단건이 아니라
     * <b>격리수준</b>이다 — 실측: 기본({@code REPEATABLE READ})에서 이 테이블을 훑는
     * {@code UPDATE} 는 X 락 <b>151</b>(전부 + supremum)을 잡아 발급 전 {@code FOR SHARE} 와
     * 재고 소진 {@code CLOSED} 가 <b>둘 다 {@code ERROR 1205}</b> 였고,
     * {@code READ COMMITTED} 에서는 {@code X,REC_NOT_GAP} 10 만 잡고 둘 다 통과했다.
     *
     * <p><b>그래서 위 락 테스트들로는 이 결정을 지킬 수 없다.</b> 집합 {@code UPDATE} 로
     * 되돌리는 돌연변이를 넣었을 때 그 셋이 전부 초록이었다 — RC 에서는 집합
     * {@code UPDATE} 도 발급을 안 막기 때문이다. 값을 지키는 것은 이 단언이다.
     *
     * <p>{@code TransactionTemplate} 이 격리수준을 안 내주므로 필드로 읽는다.
     */
    @Test
    @DisplayName("전이는 READ COMMITTED 로 돈다 — 기본 격리수준이면 발급이 전면 실패한다")
    void transitionsRunAtReadCommitted() throws Exception {
        // 쓰기·읽기 템플릿 **둘 다** 본다. 하나만 재면 나머지가 기본 격리로 새는 것을
        // 못 잡는다 — 조회도 같은 테이블을 훑으므로 같은 위험을 진다.
        TransactionTemplate write = templateField("perRoundWrite");
        TransactionTemplate read = templateField("perRoundRead");

        assertThat(write.getIsolationLevel())
                .as("기본값으로 되돌아가면 1분마다 발급 전 상태 확인과 재고 소진 마감이 "
                        + "ERROR 1205 로 죽는다")
                .isEqualTo(TransactionDefinition.ISOLATION_READ_COMMITTED);
        assertThat(read.getIsolationLevel())
                .isEqualTo(TransactionDefinition.ISOLATION_READ_COMMITTED);
        assertThat(write.getTimeout())
                .as("데드라인이 없으면 발급이 그 행을 쥔 동안 스케줄러 스레드와 커넥션을 "
                        + "innodb_lock_wait_timeout(50초)까지 물고 있는다")
                .isPositive();
        assertThat(read.getTimeout()).isPositive();
        assertThat(read.isReadOnly())
                .as("조회 손잡이는 읽기 전용이어야 한다 — 쓰기 경로와 뜻이 갈린다")
                .isTrue();
    }

    /** 이 커넥션이 {@code coupons} 에 잡고 있는 레코드 락 수. 남의 테스트 락이 안 섞이게 스레드로 거른다. */
    private int lockedCouponRecords(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement();
                var rs = statement.executeQuery("""
                        SELECT COUNT(*)
                          FROM performance_schema.data_locks
                         WHERE OBJECT_NAME = 'coupons'
                           AND LOCK_TYPE = 'RECORD'
                           AND THREAD_ID = PS_CURRENT_THREAD_ID()
                        """)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private TransactionTemplate templateField(String name) throws Exception {
        Field field = CouponRoundJdbcAdapter.class.getDeclaredField(name);
        field.setAccessible(true);
        return (TransactionTemplate) field.get(adapter);
    }

    private String statusOf(long couponId) {
        return jdbcClient.sql("SELECT status FROM coupons WHERE id = :id")
                .param("id", couponId).query(String.class).single();
    }
}

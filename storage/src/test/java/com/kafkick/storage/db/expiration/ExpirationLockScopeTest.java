// 만료 SQL 이 무엇을 잠그는지 확인합니다. 무엇을 넘겼는지는 ExpirationJdbcAdapterTest 가 봅니다.
package com.kafkick.storage.db.expiration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.expiration.ExpireChunk;
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
 * <p><b>스캔 축은 이 클래스에서 뺐다.</b> 청크가 두 문장이 되면서 그 축을 지는 문장이
 * 후보 조회로 옮겨 갔는데, <b>그 문장의 실행계획은 이 픽스처 규모에서 뜻을 잃는다</b> —
 * 200~300행짜리 표에서는 옵티마이저가 PK 스캔을 고르고, 그 규모에서는 그게 실제로 맞다.
 * 한 번은 통과하고 한 번은 실패하는 것을 겪었다(같은 단언, 같은 코드).
 * 수치와 재현 방법은 {@code docs/12-expire-lock-measurement.md} §11 에 있다 —
 * 이 클래스가 {@code lastExpiredId} 의 축을 같은 이유로 거기 둔 것과 같은 규율이다.
 *
 * <p><b>통과시키려고 뺀 것이 아니다.</b> 기준을 올리면 초록이 되지만 그때부터 그 단언은
 * 아무것도 안 막는다. 픽스처를 키우는 쪽도 재 봤는데, 그러면 이 클래스가 재는 것이
 * 락 범위가 아니라 규모가 된다.
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
    /** 픽스처보다 크다 — 청크가 잘리지 않게 하려는 것이지 무제한이라는 뜻이 아니다. */
    private static final int LIMIT_ABOVE_FIXTURE = 1000;

    /** 회차 하나가 담을 수 있는 재고. 넘기면 {@code ck_stock_range} 가 거부한다. */
    private static final int STOCK_PER_COUPON = 100;

    /** 마지막 {@link #expireChunk} 가 잡은 상한·회차. 뒤 문장들이 이 값을 받는다. */
    private long chunkBoundary;
    private long chunkCouponId;

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


    /**
     * <b>잡의 청크 한 번을 그대로 밟는다</b> — 후보 → 연속부 → 만료 → 재고 잠금.
     *
     * <p>이 클래스가 재는 것이 <b>락 범위</b>라 순서를 흉내내면 의미가 없다. 운영과 같은
     * 문장을 같은 차례로 돌린다 — 재고 잠금이 <b>마지막</b>인 것까지 포함해서다.
     *
     * <p><b>이 순서가 2026-08-30 에 뒤집혔다.</b> 발급·취소·사용취소 셋이 재고를 마지막에
     * 잡도록 옮겨졌고(CY-750), 만료만 먼저 잡으면 그 셋과 사이에 1213 이 난다
     * ({@code docs/12} §11.1). 이 헬퍼가 옛 순서를 유지하면 <b>이 클래스와
     * {@code ExpireJobLockOrderTest} 가 서로 반대를 단언하면서 둘 다 초록</b>이 된다.
     */
    private int expireChunk(long afterId, int limit, List<Long> blocked) {
        ExpireChunk chunk = ExpireChunk.from(
                adapter.nextCandidates(AS_OF, afterId, limit, blocked));
        chunkBoundary = chunk.lastId();
        chunkCouponId = chunk.couponId();
        if (chunk.isEmpty()) {
            return 0;
        }
        int expired = adapter.expireBatch(AS_OF, WROTE_AT, afterId, chunk.lastId(),
                chunk.couponId());
        if (expired > 0) {
            assertThat(adapter.lockStock(chunk.couponId()))
                    .as("재고 행이 없으면 락이 안 걸린 채로 뒤 단언이 돈다 — "
                            + "이 클래스가 재는 것이 락 수라 그때는 조용히 통과한다")
                    .isTrue();
        }
        return expired;
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

    /**
     * 어댑터를 트랜잭션으로 감싸고, 커밋으로 락이 풀리기 전에 <b>락과 읽은 행을 함께</b> 센다.
     *
     * <p><b>락만 재면 검출력이 없다.</b> READ COMMITTED 는 매치 안 된 행의 락을 즉시 놓으므로
     * 인덱스가 사라져도 락 수는 그대로 0 이다 — 실제로 {@code V11} 을 지우는 돌연변이가
     * 락 단언을 전부 통과했다. 그때 유일하게 움직이는 것이 <b>읽은 행 수</b>다.
     *
     * <p>그래서 두 축을 한 번에 돌려주고, 부르는 쪽이 둘 다 단언한다. 락 축은 격리 수준이
     * RR 로 되돌아가는 것을, 스캔 축은 인덱스가 사라지는 것을 잡는다 — 서로 못 대신한다.
     */
    private Measured measure(IntSupplier call, int expectedRows) {
        return transaction.execute(status -> {
            long before = rowsRead();
            assertThat(call.getAsInt())
                    .as("넘긴 건수가 먼저 맞아야 두 수치가 뜻을 갖는다")
                    .isEqualTo(expectedRows);
            long scanned = rowsRead() - before;
            return new Measured(lockedRecords(), scanned);
        });
    }

    /** 한 호출이 잡은 락과 읽은 행. 둘 다 있어야 인덱스와 격리를 함께 지킨다. */
    private record Measured(int locks, long scanned) {
    }

    /**
     * <b>테이블을 가리지 않고</b> 지금 트랜잭션이 잡은 레코드 락 수.
     * {@link #lockedRecords()} 는 {@code issuances} 만 세므로 <b>읽기 문장이
     * {@code coupon_stocks} 를 잡는 것을 못 본다</b> — 그 축을 재려면 이쪽이 필요하다.
     */
    private int lockedRecordsAnywhere() {
        return jdbcClient.sql("""
                        SELECT COUNT(*)
                          FROM performance_schema.data_locks
                         WHERE LOCK_TYPE = 'RECORD'
                           AND THREAD_ID = PS_CURRENT_THREAD_ID()
                        """)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>"락을 안 잡는다" 를 주석이 아니라 계측으로 고정한다.</b>
     *
     * <p>{@code ExpireJobLockOrderTest} 는 호출 <b>이름의 순서</b>만 본다. 그래서 누가
     * 성능이나 경합을 이유로 이 두 문장에 {@code FOR SHARE} 를 붙여도 그 테스트는 초록이다.
     * 그런데 {@code BLOCKED_COUPONS} 는 {@code issuances} 파생테이블에 {@code coupon_stocks}
     * 를 {@code LEFT JOIN} 하므로, 락을 잡는 순간 청크 트랜잭션 맨 앞에서
     * {@code coupon_stocks} 를 쥐게 된다 — 발급 경로와 엇갈리면 <b>1213 데드락</b>이고,
     * 그때 희생되는 쪽이 만료면 그 주기가 통째로 밀린다.
     *
     * <p>{@code countPending} 도 같다. 이쪽은 {@code afterJob} 에서 트랜잭션 밖으로 도는데,
     * 락을 잡게 되면 <b>검증이 읽는 데이터를 관측이 건드리는</b> 모양이 된다.
     */
    @Test
    @DisplayName("제외 판정과 대기 집계는 락을 잡지 않는다 — 순서 계약 밖이다")
    void readOnlyQueriesTakeNoLocks() {
        issuances(200, IssuanceStatus.ISSUED, EXPIRED_AT);

        int[] locks = transaction.execute(status -> {
            adapter.blockedCoupons(AS_OF);
            int afterBlocked = lockedRecordsAnywhere();
            adapter.countPending(AS_OF, null, List.of());
            return new int[] {afterBlocked, lockedRecordsAnywhere()};
        });

        assertThat(locks[0])
                .as("**FOR SHARE 를 붙이는 순간 여기가 움직인다.** 그러면 락 순서 계약의 "
                        + "중간(issuance_histories)을 건너뛰고 coupon_stocks 를 잡게 된다")
                .isZero();
        assertThat(locks[1])
                .as("countPending 은 afterJob 에서 트랜잭션 밖으로 돈다. 여기서 락이 잡히면 "
                        + "관측이 원본을 건드린다")
                .isZero();
    }

    /**
     * <b>{@code NOT IN} 이 붙어도 실행계획이 그대로여야 한다.</b>
     *
     * <p>{@code EXPIRE_BATCH} 의 락을 5,020 에서 2 로 떨어뜨린 것은 {@code V11
     * (status, expires_at)} 인덱스다. 술어가 하나 늘면 옵티마이저가 그 인덱스를 안 고를 수
     * 있고, 그러면 <b>발급 봉쇄가 그대로 되돌아온다.</b>
     *
     * <p><b>제외가 실제로 후보를 걸러내는 픽스처라야 뜻이 있다.</b> 처음엔 기한이 남은 행만
     * 심어 이 자리를 재려 했는데, 그러면 {@code expires_at < :asOf} 에 하나도 안 걸려
     * <b>{@code NOT IN} 이 평가될 행 자체가 없었다.</b> 락 단언도 0행 갱신이라 무엇을
     * 바꿔도 참이었다 — 깨지지 않는 단언은 없는 단언과 같다.
     *
     * <p>그래서 <b>막힌 회차에 대기를 잔뜩 쌓고</b> 성한 회차 것만 넘긴다. 이 픽스처가
     * 재는 것은 두 가지다 — 인덱스를 계속 고르는가(스캔), 그리고 <b>버릴 행까지 잠그지는
     * 않는가</b>(락). 뒤엣것이 이번 변경이 새로 만든 축이다.
     */
    @Test
    @DisplayName("제외한 회차의 대기 행을 잠그지 않는다 — 훑기는 한다")
    void keepsScanBoundedWhenExclusionFiltersCandidates() {
        int blockedRows = 40;
        long blockedCoupon = seed.newCoupon();
        for (int i = 0; i < blockedRows; i++) {
            issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        }

        int target = 5;
        seed.newCoupon();
        for (int i = 0; i < target; i++) {
            issuance(IssuanceStatus.ISSUED, EXPIRED_AT);
        }

        // 컷 밖의 행. 인덱스를 버리면 이것까지 읽으므로 스캔 축에 검출력이 생긴다.
        int outsideCut = 150;
        issuances(outsideCut, IssuanceStatus.ISSUED, ALIVE_AT);

        Measured measured = measure(
                () -> expireChunk(0L, LIMIT_ABOVE_FIXTURE, List.of(blockedCoupon)), target);

        assertThat(measured.locks())
                .as("**버릴 행을 잠그면 안 된다.** 막힌 회차의 %d 행까지 잠그면 그 회차를 "
                        + "건드리는 다른 경로가 만료가 도는 내내 막힌다", blockedRows)
                .isLessThan(target * 4);
        // 스캔 축은 여기 없다 — 클래스 주석의 그 이유다. 컷 밖에 심어 둔 행은
        // 락 단언에도 힘을 준다: 버릴 행까지 잠그면 그 수치가 바로 커진다.
    }

    /**
     * <b>제외 판정이 테이블 전체를 훑지 않아야 한다.</b>
     *
     * <p>이 질의는 매 실행 첫 청크 트랜잭션 <b>안에서</b> 돌고 {@code LIMIT} 도 id 범위도
     * 없다. 그런데 {@code V11 (status, expires_at)} 에는 {@code coupon_id} 가 없어서,
     * 옵티마이저가 {@code GROUP BY coupon_id} 를 임시테이블 없이 끝내려고
     * <b>{@code uk_coupon_member (coupon_id, member_id)} 순서로 인덱스 풀스캔</b>을 고를
     * 여지가 있다. 그러면 대기가 0건인 날에도 테이블 전체를 읽는다 —
     * <i>"대기가 없는 날은 거의 공짜"</i> 라는 전제가 정확히 뒤집힌다.
     *
     * <p>그 날 첫 청크가 {@code step-timeout-ms} 에 걸려 끊기면 만료가 <b>한 건도 못 하고</b>
     * 실패한다. 이 잡이 없애려던 <i>"그 뒤 id 의 만료가 밀린다"</i> 와 같은 자리다.
     */
    @Test
    @DisplayName("제외 판정이 대기 없는 날 테이블을 훑지 않는다")
    void keepsBlockedCouponScanProportionalToPending() {
        int rows = 200;
        issuances(rows, IssuanceStatus.ISSUED, ALIVE_AT);

        long scanned = transaction.execute(status -> {
            long before = rowsRead();
            assertThat(adapter.blockedCoupons(AS_OF))
                    .as("대기가 0건이라 막힌 회차도 없다")
                    .isEmpty();
            return rowsRead() - before;
        });

        assertThat(scanned)
                .as("uk_coupon_member 로 GROUP BY 를 푸는 계획을 고르면 %d 행을 전부 읽는다. "
                        + "이 질의는 매 실행 첫 청크 안에서 돈다", rows)
                .isLessThan(rows / 4);
    }

    /**
     * <b>넘길 것이 하나도 없는 실행이 최악이었다.</b> {@code LIMIT} 이 발동할 일이 없어 끝까지
     * 훑었고, 하나도 안 바꾸면서 지나간 행을 전부 잠갔다. 하루 288회 중 대부분이 이 경로다.
     *
     * <p><b>두 축을 함께 본다.</b> 락 축은 격리가 REPEATABLE READ 로 되돌아가는 것을,
     * 스캔 축은 인덱스가 사라지는 것을 잡는다. 실측(5,000행·만료 대상 0건·RC):
     * 인덱스 없음 <b>5,017행</b> → 인덱스 있음 <b>1행</b>. 운영 300만 행이면 매 실행이
     * 300만 행을 읽느냐 마느냐의 차이이고, 그것이 5분 주기를 지킬 수 있느냐를 가른다.
     *
     * <p>{@code Handler_read_*} 는 스토리지 엔진이 실제로 넘긴 행을 센다 —
     * {@code EXPLAIN} 의 추정치가 아니라 실행 결과다.
     */
    @Test
    @DisplayName("넘길 것이 없는 실행은 훑지도 잠그지도 않는다")
    void locksNothingWhenNothingExpires() {
        int rows = 200;
        issuances(rows, IssuanceStatus.ISSUED, ALIVE_AT);

        Measured measured = measure(() -> expireChunk(0L, LIMIT_ABOVE_FIXTURE, List.of()), 0);

        assertThat(measured.locks())
                .as("한 건도 안 넘겼으면 잠글 것도 없다. 여기가 %d 에 가까워지면 격리가 "
                        + "REPEATABLE READ 로 되돌아간 것이다", rows)
                .isLessThan(20);
        assertThat(measured.scanned())
                .as("**인덱스를 지키는 것은 이 축이다.** 위 단언은 RC 라 인덱스가 없어도 통과한다 — "
                        + "인덱스가 사라지면 %d 행을 전부 읽는다", rows)
                .isLessThan(rows / 4);
    }

    /**
     * <b>진도({@code afterId})는 JobInstance 안에서만 산다.</b> 주기 실행은 {@code asOf} 가
     * 매번 달라 새 인스턴스이므로 이미 처리가 끝난 앞 구간을 매번 새로 지나간다.
     * 예전에는 지나가면서 전부 잠갔다 — 넘길 것이 10건뿐이어도 비용이 테이블 크기를 따라갔다.
     * 인덱스가 그 구간을 아예 안 보게 만든다.
     *
     * <p><b>제목에 "넘긴 건수에만 비례한다" 고 적었었다. 그것까지는 아니다.</b> 잠그는 것은
     * <b>후보 수</b>이지 넘긴 건수가 아니다 — {@code ORDER BY id LIMIT} 은 정렬량만 묶고,
     * 잘려 나간 후보도 {@code WHERE} 를 만족하므로 RC 가 그 락을 안 놓는다. 후보가
     * {@code LIMIT} 의 몇 배인 구간에서 실측으로 확인했다(후보 5,000 · {@code LIMIT} 1,000
     * → 락 5,000). 여기서 지키는 것은 <b>이미 끝난 앞 구간을 안 본다</b> 까지다.
     *
     * <p>그 조건은 이 픽스처 규모로 못 만든다 — 행이 적으면 옵티마이저가 PK 를 골라 조기
     * 종료한다. 그래서 재현은 측정 스크립트에 두었고 수치는
     * {@code docs/12-expire-lock-measurement.md} 에 있다.
     *
     * <p><b>스캔 축은 후보 조회만 잰다.</b> 청크가 두 문장이 되면서, 이 규모에서는 만료
     * {@code UPDATE} 가 PK 를 골라 앞 구간을 지나간다 — 200행짜리 표에서는 그게 싸 보이기
     * 때문이다. 20,000행으로 늘려 재면 같은 문장이 {@code idx_issuance_status_id} 를 고른다.
     * 픽스처를 키워 옵티마이저를 달래는 대신, <b>이 계약을 지는 문장</b>을 직접 잰다.
     */
    @Test
    @DisplayName("이미 처리가 끝난 앞 구간은 잠그지 않는다")
    void locksOnlyWhatItExpires() {
        int settled = 190;
        int toExpire = 10;
        issuances(settled, IssuanceStatus.EXPIRED, EXPIRED_AT);
        issuances(toExpire, IssuanceStatus.ISSUED, EXPIRED_AT);

        Measured measured = measure(() -> expireChunk(0L, LIMIT_ABOVE_FIXTURE, List.of()), toExpire);

        assertThat(measured.locks())
                .as("%d 건만 넘겼다. 이미 끝난 %d 건까지 잠그면 격리가 되돌아간 것이다",
                        toExpire, settled)
                .isLessThan(settled / 2);
        // 앞 구간을 "훑는지" 는 여기서 안 잰다 — 클래스 주석의 그 이유다.
        // 여기서 지키는 것은 훑더라도 **잠그지는 않는다** 까지다.
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
            assertThat(expireChunk(0L, toExpire, List.of())).isEqualTo(toExpire);
            int afterFirst = lockedRecords();

            long lastId = chunkBoundary;
            assertThat(lastId).as("경계를 못 구하면 뒤 문장이 전부 빈 집합을 본다").isPositive();

            long before = rowsRead();
            adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, lastId, chunkCouponId);
            adapter.releaseStock(chunkCouponId, toExpire, WROTE_AT);
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
            assertThat(expireChunk(0L, LIMIT_ABOVE_FIXTURE, List.of()))
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
            // 커넥션은 풀로 돌아가는데 HikariCP 는 임의 세션 변수를 안 되돌린다. 안 복원하면
            // 뒤 테스트가 이 커넥션을 받아 락 대기가 2초에서 끊기고, 원인이 자기 코드가
            // 아닌 자리에서 간헐 실패한다.
            int original = sessionLockWaitTimeout(connection);
            setSessionLockWaitTimeout(connection, 2);
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO issuances
                           (coupon_id, member_id, code, issued_grade, status,
                            issued_at, expires_at, updated_at, created_at)
                    VALUES (?, ?, 'LOCKSCOPE0000001', ?, 'ISSUED', ?, ?, ?, ?)
                    """)) {
                insert.setLong(1, couponId);
                insert.setLong(2, issuer.memberId());
                insert.setString(3, issuer.grade());
                insert.setTimestamp(4, Timestamp.valueOf(AS_OF));
                insert.setTimestamp(5, Timestamp.valueOf(ALIVE_AT));
                insert.setTimestamp(6, Timestamp.valueOf(AS_OF));
                // created_at — main 의 V5 가 도메인 발급시각과 감사시각을 갈랐다(NOT NULL).
                insert.setTimestamp(7, Timestamp.valueOf(AS_OF));
                insert.executeUpdate();
            } finally {
                setSessionLockWaitTimeout(connection, original);
            }
        }
    }

    /** 지금 이 커넥션의 락 대기 초. 복원할 값을 먼저 읽어 둔다. */
    private int sessionLockWaitTimeout(Connection connection) throws Exception {
        try (Statement read = connection.createStatement();
                ResultSet rs = read.executeQuery("SELECT @@SESSION.innodb_lock_wait_timeout")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private void setSessionLockWaitTimeout(Connection connection, int seconds) throws Exception {
        try (Statement write = connection.createStatement()) {
            write.execute("SET SESSION innodb_lock_wait_timeout = " + seconds);
        }
    }

    /**
     * <b>문장마다 어느 테이블을 잠그는지 잰다 — 락 순서 계약의 근거다.</b>
     *
     * <p>{@code ExpireJobLockOrderTest} 는 저장소 <b>호출 순서</b>만 본다. 그것으로는
     * <i>"{@code releaseStock} 안에서 {@code coupon_stocks} 를 먼저 잠근다"</i> 같은 것을
     * 원리적으로 못 본다 — 한 문장이 두 테이블을 잡는 경우가 그 밖에 있다. 그 클래스의
     * 표는 <b>이 테스트가 재는 값</b>이고, 예전에는 그 표를 뒷받침하는 것이 아무것도 없었다.
     *
     * <p><b>발급 경로가 이미 쓰고 있는 순서가 여기 박힌다.</b>
     * {@code issuances} → {@code issuance_histories} → {@code coupon_stocks}.
     * {@code CouponIssueService}·{@code CouponCancelService}·{@code CouponCancelUseService} 가
     * 전부 재고를 <b>마지막</b>에 건드린다(CY-750) — 선착순 판정은 조건부 원자 UPDATE
     * ({@code occupyOne} · {@code release})가 그 자리에서 하므로, 앞에서 따로 잠글 이유가 없다.
     *
     * <p><b>이 방향이 2026-08-30 에 뒤집혔다.</b> 그전에는 재고가 먼저였고, 그때 근거는
     * <i>"발급 경로가 이미 그렇게 하고 있으니 바꿀 수 있는 쪽은 만료"</i> 였다
     * ({@code docs/12} §11). CY-750 이 그 전제를 바꿨고, §11.1 이 양방향을 다시 쟀다.
     *
     * <p><b>어느 방향이든 하나만 반대면 1213 이다.</b> MySQL 8.4 · READ COMMITTED · 두
     * 세션으로 두 번 재현했다 — 한 번은 만료만 반대였을 때, 한 번은 사용자 경로만 반대였을 때.
     * 희생되는 쪽은 언제나 <b>취소</b>다 — 그 시점까지 읽기만 해서 undo 가 비어 있고 InnoDB 는
     * 한 일이 적은 쪽을 죽인다. 사용자 요청이 죽는데 배치 로그는 깨끗하다.
     *
     * <p><b>정확한 락 개수는 못 박지 않는다.</b> 실행계획에 따라 남는 S 락이 뜨고 안 떠서
     * 단독 실행과 전체 스위트에서 값이 갈린다. 그래서 지키는 것은 <b>테이블 이름과 그 차례</b>,
     * 그리고 <b>청크에 묶이는 것</b>이지 개수의 동일성이 아니다.
     *
     * <p><b>격리 수준은 이 테스트가 안 지킨다.</b> 뒤 문장들이 읽는 행은 첫 문장이 이미
     * X 락을 잡아 둔 행이라, REPEATABLE READ 로 되돌려도 락이 새로 안 생긴다 — 실제로
     * RR 로 바꾸는 돌연변이가 여기를 통과했다. 그 축을 잡는 것은
     * {@link #issuanceInsertPassesWhileChunkIsOpen()} 이고, 그 돌연변이에서 실패한 것도
     * 그 테스트 하나였다.
     *
     * <p><b>못 재는 것이 하나 있다.</b> {@code issuance_histories} 에 <i>넣는</i> 행의 락은
     * InnoDB 가 <b>암묵적으로</b> 잡아서, 다른 세션이 부딪혀 실체화되기 전에는
     * {@code data_locks} 에 뜨지 않는다. 그래서 이 축은 <b>이력이 언제 잠기는지</b>가 아니라
     * <b>이력 INSERT 가 원본·재고를 건드리지 않는지</b>를 지킨다. 순서 계약에서 이력의 자리는
     * 호출 순서({@code ExpireJobLockOrderTest})가 맡는다 — 둘이 합쳐야 표 전체가 선다.
     */
    @Test
    @DisplayName("문장마다 잠그는 테이블이 계약대로다 — issuances → histories → stocks")
    void eachStatementLocksTheTablesItsContractSays() {
        int toExpire = 5;
        int beyond = 300;
        issuances(toExpire, IssuanceStatus.ISSUED, EXPIRED_AT);
        // 청크 밖 행을 심어야 "구간에 묶인다" 는 단언에 힘이 생긴다.
        issuances(beyond, IssuanceStatus.EXPIRED, EXPIRED_AT);

        transaction.executeWithoutResult(status -> {
            ExpireChunk chunk = ExpireChunk.from(
                    adapter.nextCandidates(AS_OF, 0L, LIMIT_ABOVE_FIXTURE, List.of()));
            assertThat(lockedTables())
                    .as("후보 조회는 락을 안 잡는다 — 이것이 성립해야 이 순서가 성립한다")
                    .isEmpty();

            assertThat(adapter.expireBatch(AS_OF, WROTE_AT, 0L, chunk.lastId(), chunk.couponId()))
                    .isEqualTo(toExpire);
            assertThat(lockedTables())
                    .as("**원본이 먼저다.** 발급·취소가 마지막에 잡는 재고 행을 우리도 "
                            + "마지막에 잡는다 — 그래야 순환이 안 생긴다(docs/12 §11.1)")
                    .containsOnlyKeys("issuances");

            int afterExpire = lockedTables().get("issuances");
            adapter.appendExpireHistories(AS_OF, WROTE_AT, 0L, chunk.lastId(), chunk.couponId());
            assertThat(lockedTables())
                    .as("이력은 자기 테이블에만 넣는다. (넣은 행 자체의 락은 암묵적이라 "
                            + "data_locks 에 안 뜬다 — 충돌이 나야 실체화된다)")
                    .containsOnlyKeys("issuances");
            assertThat(lockedTables().get("issuances"))
                    .as("이력 INSERT 가 원본으로 번지지 않는다")
                    .isEqualTo(afterExpire);

            assertThat(adapter.lockStock(chunk.couponId())).isTrue();
            assertThat(lockedTables().keySet())
                    .as("**재고는 여기서 처음 나타난다.** 이 자리보다 앞이면 발급·취소와 "
                            + "순서가 역전된다")
                    .containsExactlyInAnyOrder("coupon_stocks", "issuances");

            adapter.releaseStock(chunk.couponId(), toExpire, WROTE_AT);
            assertThat(lockedTables().get("issuances"))
                    .as("**청크에 묶인다.** 심어 둔 %d 행 근처로 커지면 구간 밖으로 번진 것이다. "
                            + "정확한 개수는 못 박지 않는다 — 실행계획에 따라 S 락이 뜨고 안 뜬다",
                            toExpire + beyond)
                    .isLessThan(toExpire * 5);
            assertThat(lockedTables().keySet())
                    .as("재고 차감은 이미 우리 것인 행을 고칠 뿐이라 새 테이블이 안 늘어난다")
                    .containsExactlyInAnyOrder("coupon_stocks", "issuances");
        });
    }

    /**
     * 지금 트랜잭션이 잡고 있는 레코드 락을 <b>테이블별로</b> 센다.
     *
     * <p>{@link #lockedRecords()} 는 {@code issuances} 하나만 본다. 락 <b>순서</b> 계약은
     * 여러 테이블에 걸쳐 있어서 그것으로는 못 지킨다.
     */
    private Map<String, Integer> lockedTables() {
        return jdbcClient.sql("""
                        SELECT OBJECT_NAME, COUNT(*)
                          FROM performance_schema.data_locks
                         WHERE LOCK_TYPE = 'RECORD'
                           AND THREAD_ID = PS_CURRENT_THREAD_ID()
                         GROUP BY OBJECT_NAME
                        """)
                .query((rs, rowNum) -> Map.entry(rs.getString(1), rs.getInt(2)))
                .list()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * 스토리지 엔진이 실제로 넘긴 행 수. 추정치가 아니라 실행 결과다.
     *
     * <p>{@code FLUSH STATUS} 로 0 부터 세는 대신 <b>전후 차이</b>로 잰다 — 그 문장은
     * {@code PreparedStatement} 로 못 돌고 {@code RELOAD} 권한도 필요하다. 재는 질의 자신이
     * 올리는 몫은 양쪽에 똑같이 섞이므로 차이에서 상쇄된다.
     */
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

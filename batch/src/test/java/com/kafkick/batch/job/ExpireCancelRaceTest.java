// 만료와 취소가 같은 발급건을 잡을 때 재고가 한 번만 돌아오는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.batch.config.FixedClock;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>{@code docs/11} 이 🔴컷 불가로 지정한 항목이다 — "만료 × 취소 → 재고 1회만 복원".</b>
 *
 * <p><b>취소 경로가 아직 없다는 것은 취소 SQL 을 못 쓴다는 뜻이지 경합을 못 재현한다는 뜻이
 * 아니다.</b> 두 번째 커넥션으로 취소가 할 일을 그대로 쓴다 — 조건부 {@code UPDATE} 로
 * {@code ISSUED} 를 잡고, <b>실제로 바꾼 행이 있을 때만</b> 재고를 되돌린다.
 * 발급·취소 티켓이 들어올 때 이 모양을 따라야 한다.
 *
 * <p><b>승패를 스레드 타이밍에 맡기지 않는다.</b> 자바 래치로 "동시 발사" 를 흉내내면
 * 잡 기동이 JobRepository 왕복을 여러 번 도는 사이 취소가 먼저 끝나서, 실제로는
 * <b>취소 단독 경로만</b> 돌고 만료는 빈손으로 끝난다. 그러면 이 테스트가 막겠다는 회귀가
 * 그대로 통과한다. 그래서 두 방향을 <b>각각 결정적으로</b> 고정한다 —
 * {@link #cancelWinsBeforeExpire()} 와 {@link #expireWinsThenCancelFindsNothing()}.
 *
 * <p><b>재고를 3 으로 둔다.</b> 1 로 두면 두 경로의 하한 가드({@code active_count >= 차감량})가
 * 값을 0 에서 클램프해서, <b>이중 복원과 정상이 같은 값으로 끝난다.</b> 3 이면
 * 정상은 2, 이중 복원은 1 로 갈린다 — 그 차이가 이 테스트의 검출력 전부다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // 되읽기가 같은 ExpirationRepository 프록시에 countPending 을 부른다 —
        // 백그라운드 틱이 호출 기록에 끼면 재현 불가로 빨개진다.
        "batch.metrics.expire-pending-initial-delay-ms=3600000",
        "batch.expire.chunk-size=1"
})
@Import({MySqlContainerConfig.class, ExpireCancelRaceTest.FixedClockConfig.class,
        ExpireCancelRaceTest.PauseAfterExpireConfig.class,
        ExpireCancelRaceTest.PauseBeforeReleaseConfig.class})
class ExpireCancelRaceTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 고정한 "지금". 태스클릿이 {@code asOf} 를 이 값과 견줘 미래면 거절한다. */
    private static final LocalDateTime NOW = AS_OF.plusMinutes(3);

    /** 하한 가드가 클램프하지 못하는 값. 정상 = 2, 이중 복원 = 1 로 갈린다. */
    private static final int INITIAL_STOCK = 3;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private VerificationSeed seed;
    private long target;
    private long sibling;
    private long couponId;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
        PauseAfterExpireConfig.reset();
        PauseBeforeReleaseConfig.reset();

        target = seed.issuance(IssuanceStatus.ISSUED);
        couponId = seed.currentCouponId();
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", target)
                .update();
        // 같은 회차에 기한이 남은 건 둘을 더 심는다. 만료 대상은 여전히 하나다.
        //
        // 재고를 3 으로 정하는 것은 아래 overwriteStock 이다. 이 둘은 **취소가 때릴 형제**를
        // 만드는 것이고(마지막 하나만 sibling 으로 쓴다), 둘 다 심는 이유는 ISSUED 행 수와
        // active_count 를 맞춰 두기 위해서다 — 어긋난 재고로 시작하면 이 테스트가 재는 것이
        // 경합인지 처음부터 깨져 있던 픽스처인지 갈리지 않는다.
        for (int i = 0; i < 2; i++) {
            long alive = seed.issuance(IssuanceStatus.ISSUED);
            jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                    .param("at", AS_OF.plusDays(30))
                    .param("id", alive)
                    .update();
            sibling = alive;
        }
        seed.overwriteStock(INITIAL_STOCK);
    }

    @AfterEach
    void tearDown() {
        PauseAfterExpireConfig.reset();
        PauseBeforeReleaseConfig.reset();
    }

    /**
     * <b>취소가 먼저 커밋한 뒤 만료가 온다.</b> 만료는 {@code status='ISSUED'} 에 매치되는 것이
     * 없어 한 건도 안 넘긴다 — 재고를 되돌리는 것은 취소 하나뿐이다.
     *
     * <p>{@code writeCount} 를 단언하는 것이 요점이다. 그것이 0 이어야 <b>만료가 실제로 빈손</b>
     * 이었다는 뜻이고, 그러지 않으면 이 케이스는 "취소만 도는" 시나리오와 구분되지 않는다.
     */
    @Test
    @DisplayName("취소가 먼저면 만료는 한 건도 안 넘기고 재고는 한 번만 돌아온다")
    void cancelWinsBeforeExpire() throws Exception {
        int changed = cancelIfStillIssued();
        assertThat(changed).as("취소가 먼저 잡았어야 이 케이스다").isEqualTo(1);

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(writeCountOf(execution))
                .as("취소된 건은 status='ISSUED' 에 안 걸린다. 여기가 1 이면 만료가 남의 건을 집은 것이다")
                .isZero();
        assertThat(statusOf(target)).isEqualTo(IssuanceStatus.CANCELLED.name());
        assertThatStockCameBackOnce();
    }

    /**
     * <b>만료가 먼저 넘긴 뒤 취소가 온다.</b> 프록시가 {@code expireBatch} 반환 직후 취소를 풀어
     * 주므로 순서가 흔들리지 않는다.
     *
     * <p><b>취소의 조건부 {@code UPDATE} 가 0행을 돌려주는 것</b>이 이 케이스의 핵심이다.
     * 취소가 "조회 → 판단 → 재고 차감" 구조였다면 그 사이에 만료가 끼어들어 <b>둘 다</b>
     * 재고를 되돌렸을 것이다. 발급·취소 티켓이 지켜야 하는 모양이 이것이다.
     */
    @Test
    @DisplayName("만료가 먼저면 취소는 0행을 보고 재고를 안 건드린다")
    void expireWinsThenCancelFindsNothing() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            PauseAfterExpireConfig.arm();
            // countDown 을 finally 에 둔다. 취소가 락에 걸리거나 데드락으로 죽으면 —
            // 즉 이 테스트가 잡아야 할 상황이 실제로 일어나면 — 그 예외가 Future 에 갇히고
            // 잡은 30초 뒤 "취소가 안 끝났다" 로 죽는다. 진짜 원인이 로그에서 사라진다.
            var cancelled = worker.submit(() -> {
                try {
                    assertThat(PauseAfterExpireConfig.EXPIRED.await(30, TimeUnit.SECONDS)).isTrue();
                    return cancelIfStillIssued();
                } finally {
                    PauseAfterExpireConfig.CANCEL_DONE.countDown();
                }
            });

            JobExecution execution = launch();

            assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
            assertThat(writeCountOf(execution)).as("만료가 그 건을 넘겼어야 이 케이스다").isEqualTo(1);
            assertThat(cancelled.get(30, TimeUnit.SECONDS))
                    .as("이미 EXPIRED 라 조건부 UPDATE 가 0행을 돌려줘야 한다. "
                            + "1 이면 취소가 종단 상태를 덮어쓴 것이다")
                    .isZero();
        } finally {
            worker.shutdownNow();
        }

        assertThat(statusOf(target)).isEqualTo(IssuanceStatus.EXPIRED.name());
        assertThatStockCameBackOnce();
    }

    /** 두 케이스가 공유하는 불변식. 어느 쪽이 이겼든 재고는 정확히 한 번 돌아온다. */
    private void assertThatStockCameBackOnce() {
        assertThat(activeCount())
                .as("%d 에서 한 번만 빠져야 한다. %d 면 두 경로가 각각 되돌린 것이다",
                        INITIAL_STOCK, INITIAL_STOCK - 2)
                .isEqualTo(INITIAL_STOCK - 1);
        assertThat(terminalHistoryCount())
                .as("종단 이력도 하나여야 한다")
                .isEqualTo(1);
    }

    /**
     * 취소 경로가 할 일을 <b>그 순서 그대로</b> 쓴다.
     *
     * <p><b>재고를 먼저 잠근다.</b> {@code CouponCancelService} 가 실제로 그렇게 한다 —
     * {@code lockStock} → 조건부 {@code UPDATE} → {@code release} → 이력. 예전에는 이 헬퍼가
     * <b>만료 쪽 순서</b>(발급건 먼저)를 쓰고 있었고, 그래서 두 순서가 어긋나 있다는 사실을
     * 이 테스트가 <b>구조적으로 볼 수 없었다.</b> 계약을 재는 것이 아니라 계약을 비켜 간 것이다.
     *
     * <p><b>조건부 {@code UPDATE} 의 반환값으로 판단하는 것이 핵심이다.</b> 먼저 조회해서
     * 상태를 확인하고 재고를 되돌리는 구조면, 그 사이에 만료가 끼어들어 둘 다 되돌린다.
     *
     * @return 실제로 취소한 건수. 0 이면 이미 다른 경로가 가져갔다는 뜻이다
     */
    private int cancelIfStillIssued() {
        return new TransactionTemplate(transactionManager).execute(status -> {
            // 재고 행을 먼저 잠근다 — 실제 취소 경로(CouponCancelService)의 첫 문장이다.
            jdbcClient.sql("SELECT coupon_id FROM coupon_stocks WHERE coupon_id = :coupon "
                            + "FOR UPDATE")
                    .param("coupon", couponId)
                    .query(Long.class)
                    .list();
            int changed = jdbcClient.sql("""
                            UPDATE issuances
                               SET status = 'CANCELLED', updated_at = :at
                             WHERE id = :id AND status = 'ISSUED'
                            """)
                    .param("at", AS_OF.plusMinutes(1))
                    .param("id", target)
                    .update();
            if (changed == 0) {
                return 0;
            }
            jdbcClient.sql("""
                            UPDATE coupon_stocks
                               SET active_count = active_count - 1, updated_at = :at
                             WHERE coupon_id = :coupon AND active_count >= 1
                            """)
                    .param("at", AS_OF.plusMinutes(1))
                    .param("coupon", couponId)
                    .update();
            jdbcClient.sql("""
                            INSERT INTO issuance_histories
                                        (issuance_id, event_type, from_status, to_status,
                                         reason, created_at)
                            VALUES (:id, 'CANCEL', 'ISSUED', 'CANCELLED', '사용자 취소', :at)
                            """)
                    .param("id", target)
                    .param("at", AS_OF.plusMinutes(1))
                    .update();
            return changed;
        });
    }

    private JobExecution launch() throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
    }

    private long writeCountOf(JobExecution execution) {
        return execution.getStepExecutions().stream()
                .mapToLong(StepExecution::getWriteCount)
                .sum();
    }

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int terminalHistoryCount() {
        return jdbcClient.sql("""
                        SELECT COUNT(*) FROM issuance_histories
                         WHERE issuance_id = :id AND event_type IN ('EXPIRE', 'CANCEL')
                        """)
                .param("id", target)
                .query(Integer.class)
                .single();
    }

    private int activeCount() {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", couponId)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>첫 청크가 커밋된 뒤 취소를 풀어 준다.</b>
     *
     * <p><b>넘긴 직후에 풀면 안 된다.</b> 그 시점은 아직 청크 트랜잭션 안이고 만료가 그 행을
     * X 락으로 쥐고 있어서, 취소의 조건부 {@code UPDATE} 가 락을 기다리다 교착한다 —
     * 실제로 그렇게 만들었다가 잡이 타임아웃으로 실패했다.
     *
     * <p>그래서 {@code chunk-size=1} 로 두고 <b>두 번째 {@code expireBatch} 호출</b>(0건을
     * 돌려주는 종료 확인)을 신호로 쓴다. 그 자리는 첫 청크가 이미 커밋된 뒤라 취소가
     * 락 없이 진행되고, "만료가 끝난 뒤 취소가 온다" 는 실제 순서와도 맞는다.
     *
     * <p>{@code arm()} 을 부른 테스트에서만 작동한다 — 다른 케이스가 이 대기에 걸리면
     * 그 테스트가 30초를 버리고 죽는다.
     */
    @TestConfiguration
    static class PauseAfterExpireConfig {

        static volatile CountDownLatch EXPIRED = new CountDownLatch(1);
        static volatile CountDownLatch CANCEL_DONE = new CountDownLatch(1);
        private static volatile boolean armed;
        private static volatile boolean committed;

        static void arm() {
            armed = true;
        }

        static void reset() {
            armed = false;
            committed = false;
            EXPIRED = new CountDownLatch(1);
            CANCEL_DONE = new CountDownLatch(1);
        }

        @Bean
        static BeanPostProcessor pauseAfterExpire() {
            return ExpirationProxies.decorating((real, method, args) -> {
                Object result = ExpirationProxies.callThrough(real, method, args);
                if (!armed) {
                    return result;
                }
                if ("expireBatch".equals(method.getName()) && (int) result > 0) {
                    // 넘겼다. 아직 이 청크의 트랜잭션 안이므로 여기서 풀면 취소가 락에 걸린다.
                    committed = true;
                } else if ("nextCandidates".equals(method.getName())
                        && ((java.util.List<?>) result).isEmpty() && committed) {
                    // **종료 신호가 옮겨 왔다.** 예전에는 expireBatch 가 0 을 돌려주는 것을
                    // 신호로 썼는데, 이제 후보가 비면 그 호출 자체가 없다. 후보 0 이 곧
                    // "앞 청크가 커밋된 뒤" 이고, 여기서 풀면 취소가 락 없이 지나간다.
                    EXPIRED.countDown();
                    if (!CANCEL_DONE.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("취소가 30초 안에 안 끝났다");
                    }
                }
                return result;
            });
        }
    }

    /**
     * <b>진짜로 겹치는 경우다.</b> 앞 두 케이스는 순서를 갈라 놓았을 뿐 두 트랜잭션이 같은
     * 시간에 살아 있지 않다. 만료 청크가 <b>열려 있는 동안</b> 재고가 움직이는 경로는
     * 저장소 어디에서도 검사한 적이 없었다.
     *
     * <p><b>대상을 같은 발급건으로 두면 안 된다.</b> 그 행은 {@code expireBatch} 가 X 락으로
     * 쥐고 있어 취소가 락을 기다리다 교착한다 — 실제로 그렇게 만들었다가 잡이 죽었다.
     * 위험한 조합은 <b>같은 회차의 다른 발급건</b>이다. 발급건 쪽 락은 안 겹치지만
     * <b>재고 행이 겹친다.</b>
     *
     * <p><b>이 자리의 답이 두 번 바뀌었다.</b> 처음에는 만료가 재고를 마지막에 잡아 취소가
     * 락 없이 통과했고, 그다음 만료를 청크 시작으로 옮겨 취소가 <b>기다리게</b> 했다.
     * <b>지금은 다시 마지막이다</b> — 발급·취소·사용취소 셋이 CY-750 에서 재고를 마지막으로
     * 옮겼고, 만료만 반대로 두면 그 셋과 사이에 1213 이 난다({@code docs/12} §11).
     *
     * <p>그래서 이 테스트가 재는 것도 되돌아간다. <b>두 트랜잭션이 정말로 겹치고</b>, 그래도
     * 재고가 맞는 이유는 <b>양쪽이 상대 차감</b>({@code active_count - N})을 쓰기 때문이다.
     * 절대값으로 바뀌면 겹치는 순간 한쪽이 덮어써 한 번만 빠진다 — 마지막 단언이 그것을 잡는다.
     *
     * <p><b>기다림을 단언하지 않는다.</b> 이제 기다릴 이유가 없다 — 만료는 그 시점에 재고 행을
     * 안 쥐고 있고, 취소가 건드리는 발급건도 만료 대상과 다르다. 없는 성질을 단언하면 그것이
     * 락을 되돌리라는 압력이 된다.
     */
    @Test
    @DisplayName("청크가 열린 동안 같은 회차의 취소가 끼어들어도 재고는 둘 다 빠진다")
    void concurrentCancelOnSiblingOverlapsAndBothDeduct() throws Exception {
        // 스레드 둘이 필요하다 — 하나는 취소, 하나는 잡. 본문은 그 사이에서 판정만 한다.
        ExecutorService worker = Executors.newSingleThreadExecutor();
        ExecutorService worker0 = Executors.newSingleThreadExecutor();
        try {
            PauseBeforeReleaseConfig.arm();
            var cancelled = worker.submit(() -> {
                assertThat(PauseBeforeReleaseConfig.PAUSED.await(30, TimeUnit.SECONDS)).isTrue();
                return cancelSibling();
            });
            var job = worker0.submit(this::launch);

            assertThat(PauseBeforeReleaseConfig.PAUSED.await(30, TimeUnit.SECONDS))
                    .as("청크가 재고 차감 직전에서 멈춰야 이 겹침을 만들 수 있다")
                    .isTrue();

            // **여기가 요지다.** 만료가 청크를 열어 둔 채 멈춰 있는데도 취소가 끝난다 —
            // 재고 행을 안 쥐고 있기 때문이다. 두 트랜잭션이 정말로 겹친 상태이고,
            // 그래도 아래 재고 단언이 맞는 이유는 양쪽이 상대 차감을 쓰기 때문이다.
            assertThat(cancelled.get(30, TimeUnit.SECONDS))
                    .as("만료가 재고를 안 쥐고 있으므로 취소가 끼어들 수 있다. 여기서 "
                            + "멈추면 만료가 재고를 미리 잡은 것이고, 그때는 발급·취소 "
                            + "경로와 순서가 역전돼 1213 이 난다")
                    .isEqualTo(1);

            PauseBeforeReleaseConfig.RESUME.countDown();

            JobExecution execution = job.get(60, TimeUnit.SECONDS);
            assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        } finally {
            worker.shutdownNow();
            worker0.shutdownNow();
        }

        assertThat(activeCount())
                .as("만료 1건 + 취소 1건 = %d 에서 둘이 빠져 %d 여야 한다. "
                        + "%d 면 두 경로가 같은 값을 읽고 각각 써서 한 번만 빠진 것이다",
                        INITIAL_STOCK, INITIAL_STOCK - 2, INITIAL_STOCK - 1)
                .isEqualTo(INITIAL_STOCK - 2);
    }

    /** 만료 대상이 아닌 형제 건을 취소한다. 회차가 같으므로 재고는 같은 행을 건드린다. */
    private int cancelSibling() {
        return new TransactionTemplate(transactionManager).execute(status -> {
            // 재고 행을 먼저 잠근다 — 실제 취소 경로(CouponCancelService)의 첫 문장이다.
            jdbcClient.sql("SELECT coupon_id FROM coupon_stocks WHERE coupon_id = :coupon "
                            + "FOR UPDATE")
                    .param("coupon", couponId)
                    .query(Long.class)
                    .list();
            int changed = jdbcClient.sql("""
                            UPDATE issuances
                               SET status = 'CANCELLED', updated_at = :at
                             WHERE id = :id AND status = 'ISSUED'
                            """)
                    .param("at", AS_OF.plusMinutes(1))
                    .param("id", sibling)
                    .update();
            if (changed == 0) {
                return 0;
            }
            jdbcClient.sql("""
                            UPDATE coupon_stocks
                               SET active_count = active_count - 1, updated_at = :at
                             WHERE coupon_id = :coupon AND active_count >= 1
                            """)
                    .param("at", AS_OF.plusMinutes(1))
                    .param("coupon", couponId)
                    .update();
            return changed;
        });
    }


    /**
     * 취소 세션이 <b>{@code coupon_stocks} 에서</b> 락을 기다리는 상태가 될 때까지 기다린다.
     *
     * <p>{@code data_lock_waits} 는 <b>실제로 블록된 요청만</b> 담는다. 그래서 이 값이 1 이
     * 되는 것은 취소가 그 행을 요구했고 못 받았다는 관측이다 — 스레드가 늦은 것과 갈린다.
     * 이 클래스가 {@code lockedTables()} 로 이미 쓰는 것과 같은 방식이다.
     */
    private void awaitStockLockWait() {
        Awaitility.await("취소가 coupon_stocks 락 대기에 들어가기")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(50))
                .untilAsserted(() -> assertThat(stockLockWaits())
                        .as("취소가 coupon_stocks 에서 블록되지 않았다. 만료가 그 행을 안 잡고 "
                                + "있다는 뜻이고, 그러면 이 테스트가 재려던 것이 사라진다")
                        .isPositive());
    }

    /** {@code coupon_stocks} 행을 기다리다 <b>실제로 블록된</b> 요청 수. */
    private int stockLockWaits() {
        Integer waits = jdbcClient.sql("""
                        SELECT COUNT(*)
                          FROM performance_schema.data_lock_waits w
                          JOIN performance_schema.data_locks b
                            ON b.ENGINE_LOCK_ID = w.BLOCKING_ENGINE_LOCK_ID
                         WHERE b.OBJECT_NAME = 'coupon_stocks'
                        """)
                .query(Integer.class)
                .single();
        return waits == null ? 0 : waits;
    }

    /**
     * <b>{@code releaseStock} 직전에 멈춘다.</b> 그 자리는 청크 트랜잭션이 <b>열려 있고</b>
     * 만료가 이미 대상 행을 넘긴 뒤이며, <b>재고 행은 우리가 쥐고 있다.</b>
     *
     * <p><b>재개 신호의 주인이 바뀌었다.</b> 예전에는 <i>취소가 끝나면</i> 청크가 이어 갔다 —
     * 그때는 취소가 재고 행을 락 없이 지나갈 수 있어서 그 순서가 성립했다. 이제는 취소가
     * <b>우리 락을 기다리므로</b> 그 신호를 기다리면 서로 기다리다 시한에 걸린다.
     * 그래서 <b>본문이</b> 재개를 신호한다.
     *
     * <p>{@code appendExpireHistories} 뒤에 건다 — 청크 순서가
     * {@code lockStock → expireBatch → appendExpireHistories → releaseStock} 이라
     * 그 자리가 곧 "재고 차감 직전" 이다.
     */
    @TestConfiguration
    static class PauseBeforeReleaseConfig {

        static volatile CountDownLatch PAUSED = new CountDownLatch(1);
        static volatile CountDownLatch RESUME = new CountDownLatch(1);
        private static volatile boolean armed;

        static void arm() {
            armed = true;
        }

        static void reset() {
            armed = false;
            PAUSED = new CountDownLatch(1);
            RESUME = new CountDownLatch(1);
        }

        @Bean
        static BeanPostProcessor pauseBeforeRelease() {
            return ExpirationProxies.decorating((real, method, args) -> {
                Object result = ExpirationProxies.callThrough(real, method, args);
                if (armed && "appendExpireHistories".equals(method.getName())) {
                    PAUSED.countDown();
                    if (!RESUME.await(30, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("본문이 30초 안에 재개를 안 줬다");
                    }
                }
                return result;
            });
        }
    }

    /**
     * <b>시계를 고정한다.</b> 태스클릿은 {@code asOf} 가 현재보다 미래면
     * {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는다. 벽시계로 두면 이 클래스가 재는 축과 무관한
     * 이유(<b>실행하는 날짜</b>)로 결과가 갈린다 — 형제 만료 테스트들이 이미 그렇게 한다.
     *
     * <p>운영 {@code TimeConfig} 가 {@code systemUTC} 라는 것은 {@link FixedClock} 이 진다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfig {

        @Bean
        @Primary
        Clock fixedClock() {
            return FixedClock.at(NOW);
        }
    }
}

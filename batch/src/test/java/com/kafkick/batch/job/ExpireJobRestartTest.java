// 잡이 중간에 죽었다가 다시 도는 경로를 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.FixedClock;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>재시작은 코드 읽기로만 서 있었다.</b> {@code ExpireJobTest} 는 진도가 문맥에 남는 것까지
 * 보지만 실제로 죽였다가 다시 돌리지는 않는다. 여기서 그 경로를 밟는다.
 *
 * <p><b>죽는 자리에서 위험한 것은 둘이고 방향이 반대다.</b>
 *
 * <ul>
 *   <li><b>이중 차감</b> — 이미 넘긴 청크를 재시작이 다시 집으면 재고가 두 번 돌아온다.
 *   <li><b>누락</b> — 롤백된 청크의 진도가 살아남으면 그 구간을 건너뛰어 <b>영영 안 넘어간다.</b>
 *       이쪽이 더 조용하다. 잡은 초록으로 끝나고, 남은 발급건은 기한이 지났는데도 살아 있다.
 * </ul>
 *
 * <p>재고 하나로 둘을 다 잡는다. 대상 다섯에 재고 다섯이면 <b>정확히 0</b> 이어야 한다 —
 * 이중 차감이면 모자라고 누락이면 남는다.
 *
 * <p><b>예전에 여기 "재시작이 진도를 이어받지 않는다" 고 적혀 있었다. 틀렸다.</b>
 * 그때 {@code JobRepository} 가 {@code ResourcelessJobRepository} 여서 <b>아무것도 저장되지
 * 않고 있었다</b> — {@code BATCH_JOB_EXECUTION} 이 0행이었고 {@code instanceId} 는 언제나 1
 * 이었다. "저장은 되는데 복원이 안 된다" 는 진단 자체가 그 상태의 부산물이었다.
 * {@code BatchJobRepositoryConfig} 가 JDBC 저장소를 배선한 뒤로 <b>진도가 이어진다.</b>
 *
 * <p><b>이어받는 것이 옳다.</b> 진도({@code putLong})는 가드 셋을 전부 통과한 청크 끝에서만
 * 옮겨지므로, 롤백된 청크의 진도는 애초에 안 남는다. 그래서 재시작은 <b>커밋된 데까지만</b>
 * 건너뛴다 — 위의 "누락" 위험이 여기서 닫힌다.
 *
 * <p><b>주기 실행은 여전히 0 부터다.</b> 스케줄러가 {@code asOf} 를 분 단위로 새로 잡으므로
 * 주기마다 <b>다른 JobInstance</b> 이고, 진도는 인스턴스 안에서만 산다. 이어받는 것은
 * <b>같은 {@code asOf} 를 다시 돌릴 때</b>뿐이다 — 그것이 곧 재시작이다.
 * ({@code rescanFromScratchDeductsOnce} 가 0 부터 다시 훑는 쪽을 따로 지킨다.)
 *
 * <p><b>실패는 청크 경계에서 준다.</b> {@code expireBatch} 를 세 번째 호출부터 던지게 만들면
 * 청크 둘이 커밋된 뒤 셋째가 롤백된다. 청크 크기를 1 로 둔 것은 그 경계를 눈에 보이게
 * 하기 위해서다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=1"
})
@Import({MySqlContainerConfig.class, ExpireJobRestartTest.FixedClockConfig.class, ExpireJobRestartTest.FailAtChunkConfig.class})
class ExpireJobRestartTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /** 고정한 "지금". 태스클릿이 asOf 를 이 값과 견주므로 벽시계면 실행 날짜에 딸린다. */
    private static final LocalDateTime NOW = AS_OF.plusDays(1);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
        FailAtChunkConfig.neverFail();
    }

    /**
     * <b>죽은 청크의 대상을 재시작이 다시 집어야 하고, 넘긴 청크는 다시 집으면 안 된다.</b>
     * 재고가 그 둘을 한 값으로 말해 준다.
     */
    @Test
    @DisplayName("중간에 죽었다 다시 돌면 빠짐도 겹침도 없이 전부 넘어간다")
    void resumeWithoutSkippingOrRepeating() throws Exception {
        List<Long> targets = expiredIssuances(5);
        seed.overwriteStock(5);

        FailAtChunkConfig.failFromCall(3);
        JobExecution first = launch();

        assertThat(first.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(expiredCount())
                .as("청크 둘이 커밋되고 셋째가 롤백됐다")
                .isEqualTo(2);
        assertThat(activeCount())
                .as("커밋된 둘만큼만 재고가 돌아왔다")
                .isEqualTo(3);

        assertThat(FailAtChunkConfig.scannedFrom())
                .as("첫 실행은 0 부터 훑고 청크마다 진도를 옮긴다")
                .containsExactly(0L, targets.get(0), targets.get(1));

        FailAtChunkConfig.neverFail();
        JobExecution second = launch();

        assertThat(second.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(second.getJobInstance().getInstanceId())
                .as("같은 파라미터라 새 인스턴스가 아니라 죽은 인스턴스를 이어받는다")
                .isEqualTo(first.getJobInstance().getInstanceId());
        assertThat(FailAtChunkConfig.scannedFrom())
                .as("**죽은 자리에서 이어받는다.** 커밋된 청크 둘(%s, %s)은 다시 안 집는다 — "
                        + "0 이 다시 나오면 저장소 배선이 풀려 메타데이터가 안 남는 상태로 "
                        + "돌아간 것이다", targets.get(0), targets.get(1))
                .startsWith(targets.get(1))
                .doesNotContain(0L);
        assertThat(expiredCount()).isEqualTo(5);
        assertThat(activeCount())
                .as("다섯이 정확히 한 번씩 빠졌다. 겹치면 모자라고 빠뜨리면 남는다")
                .isZero();
        assertThat(expireHistoryCount())
                .as("이력도 건마다 하나씩. 다시 집은 청크가 있으면 여기서 늘어난다")
                .isEqualTo(5);
        for (long id : targets) {
            assertThat(statusOf(id)).isEqualTo("EXPIRED");
        }
    }

    /**
     * <b>진도는 최적화이지 정합성의 근거가 아니다.</b> 문맥을 통째로 잃고 {@code id > 0} 부터
     * 다시 훑어도 결과가 같아야 한다 — 지키는 것은 {@code EXPIRE_BATCH} 의
     * {@code status = 'ISSUED'} 조건이다.
     *
     * <p>새 {@code asOf} 로 띄우면 새 JobInstance 라 진도가 0 부터 시작한다. 문맥을 잃은
     * 재시작과 같은 조건이다.
     */
    @Test
    @DisplayName("진도를 잃고 처음부터 다시 훑어도 재고가 두 번 빠지지 않는다")
    void rescanFromScratchDeductsOnce() throws Exception {
        expiredIssuances(3);
        seed.overwriteStock(3);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(activeCount()).isZero();

        JobExecution again = launchAt(AS_OF.plusMinutes(1));

        assertThat(again.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(activeCount())
                .as("이미 EXPIRED 라 매치되지 않는다. 멱등성은 진도가 아니라 SQL 이 지킨다")
                .isZero();
        assertThat(expireHistoryCount())
                .as("이력도 늘지 않는다")
                .isEqualTo(3);
    }

    /**
     * <b>재시작은 제외 목록을 다시 구해야 한다.</b>
     *
     * <p>제외 목록은 Step 의 {@code ExecutionContext} 에 실리는데, 그 문맥은 청크 커밋마다
     * 영속되고 <b>재시작이 그대로 복원한다.</b> 그래서 아무 표식 없이 캐시하면 "실행당 한 번"
     * 이 아니라 <b>"JobInstance 당 한 번"</b> 이 된다 — 첫 실행과 재시작 사이에 새로 어긋난
     * 회차를 낡은 목록이 못 보고, 그 회차에서 또 죽고, 다음 재시작도 같은 낡은 목록을
     * 복원해 <b>같은 자리에서 영원히 죽는다.</b> 이 티켓이 없앤 모양이 재시작 축에 그대로
     * 남는 것이다.
     *
     * <p>반대 방향도 나쁘다 — 그 사이 사람이 재고를 고친 회차가 계속 제외된 것으로 잡혀
     * {@code cy_expire_blocked_pending} 이 부풀고, 그만큼 누락 알림이 침묵한다.
     *
     * <p>그래서 값에 {@code JobExecution} 세대를 함께 싣고, 세대가 다르면 다시 계산한다.
     */
    @Test
    @DisplayName("재시작은 제외 목록을 다시 구한다 — 낡은 목록으로 같은 자리에서 안 죽는다")
    void recomputesExclusionOnRestart() throws Exception {
        List<Long> healthy = expiredIssuances(5);
        seed.overwriteStock(5);

        FailAtChunkConfig.failFromCall(3);
        JobExecution first = launch();
        assertThat(first.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(expiredCount())
                .as("첫 실행에는 어긋난 회차가 없어 제외 목록이 비어 있었다")
                .isEqualTo(2);

        // 첫 실행과 재시작 사이에 회차 하나가 어긋난다. 낡은 목록에는 이것이 없다.
        long broken = seed.newCoupon();
        long brokenFirst = expiredIssuance();
        long brokenSecond = expiredIssuance();
        seed.overwriteStock(1);

        FailAtChunkConfig.neverFail();
        JobExecution second = launch();

        assertThat(second.getStatus())
                .as("**낡은 목록을 쓰면 어긋난 회차가 창 안으로 들어와 STOCK_UNDERFLOW 로 "
                        + "죽는다.** 그리고 다음 재시작도 같은 목록을 복원해 또 죽는다")
                .isEqualTo(BatchStatus.COMPLETED);
        assertThat(statusOf(brokenFirst))
                .as("다시 구한 목록에는 회차 %d 가 들어 있어야 한다", broken)
                .isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(statusOf(brokenSecond)).isEqualTo(IssuanceStatus.ISSUED.name());
        assertThat(statusOf(healthy.get(4)))
                .as("성한 회차의 나머지는 그대로 넘어간다")
                .isEqualTo(IssuanceStatus.EXPIRED.name());
    }

    private long expiredIssuance() {
        long id = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.minusDays(1))
                .param("id", id)
                .update();
        return id;
    }

    private List<Long> expiredIssuances(int count) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(expiredIssuance());
        }
        return ids;
    }

    private JobExecution launch() throws Exception {
        return launchAt(AS_OF);
    }

    private JobExecution launchAt(LocalDateTime asOf) throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", asOf)
                .toJobParameters());
    }

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int expiredCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM issuances WHERE status = 'EXPIRED'")
                .query(Integer.class)
                .single();
    }

    private int expireHistoryCount() {
        return jdbcClient.sql(
                        "SELECT COUNT(*) FROM issuance_histories WHERE event_type = 'EXPIRE'")
                .query(Integer.class)
                .single();
    }

    private int activeCount() {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", seed.currentCouponId())
                .query(Integer.class)
                .single();
    }

    /**
     * 정해진 호출 차례부터 {@code expireBatch} 가 던진다. 나머지 동작은 실제 저장소 그대로다.
     *
     * <p>첫 문장에서 던져야 그 청크가 통째로 롤백된다. 뒤 문장에서 던지면 앞 문장이 이미
     * 같은 트랜잭션에 쌓여 있어 <b>같은 것을 재는 것처럼 보여도 다른 상황</b>이 된다.
     */
    @TestConfiguration
    static class FailAtChunkConfig {

        private static final AtomicInteger CALLS = new AtomicInteger();

        /** 각 청크가 어디서부터 훑기 시작했는지. 재시작이 진도를 이어받았는지의 증거다. */
        private static final List<Long> AFTER_IDS = new CopyOnWriteArrayList<>();

        private static volatile int failFrom = Integer.MAX_VALUE;

        static void failFromCall(int call) {
            reset();
            failFrom = call;
        }

        static void neverFail() {
            reset();
            failFrom = Integer.MAX_VALUE;
        }

        private static void reset() {
            CALLS.set(0);
            AFTER_IDS.clear();
        }

        static List<Long> scannedFrom() {
            return List.copyOf(AFTER_IDS);
        }

        @Bean
        static BeanPostProcessor failingExpireBatch() {
            return ExpirationProxies.decorating((real, method, args) -> {
                if ("expireBatch".equals(method.getName())) {
                    AFTER_IDS.add(afterIdOf(method, args));
                    if (CALLS.incrementAndGet() >= failFrom) {
                        throw new IllegalStateException("죽은 척한다");
                    }
                }
                return ExpirationProxies.callThrough(real, method, args);
            });
        }

        /**
         * <b>인자 위치를 인덱스로 집는 것이 이 테스트의 급소다.</b>
         * {@code expireBatch(asOf, committedAt, afterId, limit, blockedCoupons)} 에서
         * 파라미터가 하나 늘거나 순서가 바뀌면, {@code afterId} 와 {@code limit} 이 둘 다
         * 정수라 <b>캐스팅이 조용히 성공한다</b> — {@code AFTER_IDS} 에 진도가 아닌 값이
         * 쌓이고 진도 단언이 <b>거짓으로 통과</b>한다. 시그니처가 바뀐 그 자리에서 멈추게 한다.
         *
         * <p>실제로 한 번 걸렸다 — 회차 격리가 다섯째 파라미터를 더했을 때 이 가드가 먼저 울렸다.
         */
        private static long afterIdOf(Method method, Object[] args) {
            // 파라미터 이름으로는 못 본다 — core 모듈은 -parameters 없이 컴파일돼
            // arg0..arg4 로만 남는다. 대신 타입 배치를 그대로 요구한다.
            Class<?>[] types = method.getParameterTypes();
            boolean sameShape = types.length == 5 && args.length == 5
                    && types[2] == long.class && types[3] == int.class
                    && types[4] == List.class;
            if (!sameShape) {
                throw new IllegalStateException(
                        "expireBatch 시그니처가 바뀌었다. 3번째 인자가 afterId 라는 전제로 진도를 "
                                + "읽고 있으니 이 프록시부터 고쳐라. 지금 시그니처=" + method);
            }
            return (Long) args[2];
        }
    }

    /**
     * <b>시계를 고정한다.</b> 태스클릿은 {@code asOf} 가 현재보다 미래면
     * {@code EXPIRE_ASOF_IN_FUTURE} 로 죽는다. 벽시계로 두면 이 테스트가 재는 축과 무관한
     * 이유(<b>실행하는 날짜</b>)로 결과가 갈린다.
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

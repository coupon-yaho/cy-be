// 잡이 중간에 죽었다가 다시 도는 경로를 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
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
import org.springframework.jdbc.core.simple.JdbcClient;

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
 * <p><b>여기서 실측으로 드러난 것이 하나 있다 — 재시작이 진도를 이어받지 않는다.</b>
 * 죽은 실행의 {@code expire.afterId} 는 JobRepository 에 제대로 저장되는데(직접 읽어 확인했다),
 * 다음 실행의 {@link org.springframework.batch.infrastructure.item.ExecutionContext} 에는 그 키가
 * 아예 없이 시작한다. {@code JobOperator.start} 와 {@code JobOperator.restart} 가 똑같았고,
 * 같은 JobInstance 를 이어받는 것까지는 맞다. Spring Batch 6.0.4 · Boot 4 조합이다.
 *
 * <p><b>그래도 결과는 정확하다.</b> 멱등성을 지키는 것이 진도가 아니라
 * {@code EXPIRE_BATCH} 의 {@code status = 'ISSUED'} 조건이기 때문이다 — 이미 넘어간 앞 구간은
 * 다시 훑어도 매치되지 않는다. 잃는 것은 <b>그 앞 구간을 다시 훑는 비용</b>뿐이다.
 *
 * <p>그래서 아래 단언은 <b>이어받지 않는다</b> 를 확인한다. 지금 사실을 못 박아 두는 것이고,
 * 어느 날 이어받게 되면 빨간불로 알려 준다.
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
@Import({MySqlContainerConfig.class, ExpireJobRestartTest.FailAtChunkConfig.class})
class ExpireJobRestartTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

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
                .as("진도를 이어받지 않고 0 부터 다시 훑는다. 이어받게 되면 여기가 먼저 운다")
                .startsWith(0L);
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

    private List<Long> expiredIssuances(int count) {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long id = seed.issuance(IssuanceStatus.ISSUED);
            jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                    .param("at", AS_OF.minusDays(1))
                    .param("id", id)
                    .update();
            ids.add(id);
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
         * {@code expireBatch(asOf, committedAt, afterId, limit)} 에서 파라미터가 하나
         * 늘거나 순서가 바뀌면, {@code afterId} 와 {@code limit} 이 둘 다 정수라
         * <b>캐스팅이 조용히 성공한다</b> — {@code AFTER_IDS} 에 진도가 아닌 값이 쌓이고
         * 진도 단언이 <b>거짓으로 통과</b>한다. 시그니처가 바뀐 그 자리에서 멈추게 한다.
         */
        private static long afterIdOf(Method method, Object[] args) {
            // 파라미터 이름으로는 못 본다 — core 모듈은 -parameters 없이 컴파일돼
            // arg0..arg3 로만 남는다. 대신 타입 배치를 그대로 요구한다.
            Class<?>[] types = method.getParameterTypes();
            boolean sameShape = types.length == 4 && args.length == 4
                    && types[2] == long.class && types[3] == int.class;
            if (!sameShape) {
                throw new IllegalStateException(
                        "expireBatch 시그니처가 바뀌었다. 3번째 인자가 afterId 라는 전제로 진도를 "
                                + "읽고 있으니 이 프록시부터 고쳐라. 지금 시그니처=" + method);
            }
            return (Long) args[2];
        }
    }
}

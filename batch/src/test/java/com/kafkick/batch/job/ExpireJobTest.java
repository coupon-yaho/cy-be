// 만료 잡이 잡 수준에서 무엇을 보장하는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
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
 * <b>어댑터 테스트는 문장 하나씩을 본다.</b> 여기서는 청크가 <b>한 실행 안에서 이어지는지</b>를
 * 본다 — 진도가 {@code ExecutionContext} 에 실려야 다음 청크가 앞 구간을 다시 안 훑는다.
 *
 * <p><b>진도는 JobInstance 안에서만 산다.</b> 주기 실행은 {@code asOf} 가 매번 달라 0 부터
 * 시작하고, 같은 {@code asOf} 로 다시 돌리는 재시작만 이어받는다 —
 * {@code ExpireJobRestartTest} 가 그 둘을 갈라서 잰다. 여기서 보는 것은 <b>한 실행 안</b>의
 * 이어짐이다.
 *
 * <p><b>재고가 어긋난 회차를 만났을 때의 계약은 여기 없다.</b> 예전에는 그것이 잡을
 * 실패시켜서 이 클래스가 두 케이스로 지켰는데, 지금은 그 회차만 창 밖으로 빠지고 배치는
 * 정상 종료한다 — {@code ExpireBlockedCouponIsolationTest} 가 그 축을 통째로 맡는다.
 *
 * <p>청크 크기를 1 로 두어 여러 번 돌게 만든다. 기본값(1000)이면 한 번에 끝나
 * 이어지는지 아닌지를 구분할 수 없다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.expire.chunk-size=1"
})
@Import({MySqlContainerConfig.class, ExpireJobTest.FixedClockConfig.class})
class ExpireJobTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    /**
     * <b>잡이 이력에 찍을 시각.</b> 시계를 고정해 두면 이 값을 그대로 단언할 수 있다 —
     * 벽시계로 재면 "{@code AS_OF} 보다 뒤" 정도밖에 못 보고, 그 단언은 테스트를 돌리는
     * 날짜가 {@code AS_OF} 를 지났다는 우연에 기댄다. {@code AS_OF} 를 미래로 옮기는 순간
     * 코드가 멀쩡해도 빨개진다.
     */
    private static final LocalDateTime WROTE_AT = AS_OF.plusMinutes(3);

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
    }

    @Test
    @DisplayName("청크가 이어져 대상 전부를 넘긴다")
    void carryAcrossChunks() throws Exception {
        List<Long> targets = expiredIssuances(3);
        seed.overwriteStock(3);

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(expireStep(execution).getWriteCount())
                .as("청크 크기가 1 이라 세 번 돌아야 셋을 넘긴다")
                .isEqualTo(3);
        for (long id : targets) {
            assertThat(statusOf(id)).isEqualTo("EXPIRED");
        }
        assertThat(activeCount()).as("셋을 넘겼으니 3 에서 셋이 빠진다").isZero();
    }

    /**
     * <b>진도가 남아야 다음 청크가 앞 구간을 안 훑는다.</b> 이 값이 실행 사이에서 어떻게
     * 되는지는 {@code ExpireJobRestartTest} 가 따로 확인한다.
     */
    @Test
    @DisplayName("진도가 실행 문맥에 남는다")
    void keepProgressInExecutionContext() throws Exception {
        List<Long> targets = expiredIssuances(2);
        seed.overwriteStock(2);

        JobExecution execution = launch();

        assertThat(expireStep(execution).getExecutionContext()
                .getLong(ExpireJobConfig.AFTER_ID_KEY))
                .as("마지막으로 넘긴 id 가 남아야 거기서 이어 간다")
                .isEqualTo(targets.get(targets.size() - 1));
    }

    /**
     * <b>넘길 것이 없어도 실패가 아니다.</b> 5분마다 도는 잡이라 대부분의 실행은 0건이다.
     * 그것을 실패로 처리하면 알림이 하루 288번 울린다.
     */
    @Test
    @DisplayName("넘길 대상이 없으면 아무것도 안 하고 정상 종료한다")
    void completeWithoutTargets() throws Exception {
        // 기한을 앞으로 민다. 시드 기본이 발급 + 7일 이라 그대로 두면 이 asOf 기준으로
        // 이미 만료 대상이고, 그러면 "대상이 없다" 를 시험하지 못한다.
        long alive = seed.issuance(IssuanceStatus.ISSUED);
        jdbcClient.sql("UPDATE issuances SET expires_at = :at WHERE id = :id")
                .param("at", AS_OF.plusDays(1))
                .param("id", alive)
                .update();

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(expireStep(execution).getWriteCount()).isZero();
    }

    /**
     * <b>이력 시각을 {@code asOf} 로 백데이트하면 리플레이가 순서를 뒤집는다.</b>
     *
     * <p>리플레이는 {@code created_at} 을 1순위로 접는다. 잡이 도는 동안 들어온 이력보다
     * 우리 이력이 앞서게 찍히면 나중 일이 먼저 접혀 {@code USED → EXPIRE} 같은 불가능한
     * 전이가 만들어진다 — V4 와 V3 가 같이 운다.
     *
     * <p>어댑터 테스트로는 이것을 못 잡는다. 되돌리는 실수가 나는 자리는 <b>Step 이 무엇을
     * 넘기는가</b>이기 때문이다.
     */
    @Test
    @DisplayName("만료 이력은 asOf 가 아니라 실제로 쓴 시각을 갖는다")
    void writeHistoryAtRealTime() throws Exception {
        List<Long> targets = expiredIssuances(1);
        seed.overwriteStock(1);

        launch();

        assertThat(historyCreatedAt(targets.get(0)))
                .as("asOf 로 찍으면 잡이 도는 동안 들어온 이력보다 앞서게 된다. "
                        + "시계를 고정했으니 '뒤에 있다' 가 아니라 **정확히 그 값**을 본다")
                .isEqualTo(WROTE_AT);
    }

    /**
     * <b>{@code asOf} 가 없으면 조용히 성공한다.</b> {@code expires_at < NULL} 은 아무 행도
     * 매치하지 않아 "넘길 대상이 없다" 와 구분되지 않는다 — 5분마다 정상 완료로 보이면서
     * 아무것도 안 하는 상태가 된다. 검증기가 그것을 시작 전에 막는다.
     */
    @Test
    @DisplayName("asOf 없이 띄우면 시작 전에 막힌다")
    void rejectMissingAsOf() {
        expiredIssuances(1);

        assertThatThrownBy(() -> jobOperator.start(expireJob, new JobParametersBuilder()
                .toJobParameters()))
                .as("막지 않으면 아무것도 안 하고 COMPLETED 로 끝난다")
                .isInstanceOf(InvalidJobParametersException.class);
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
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
    }

    private StepExecution expireStep(JobExecution execution) {
        return execution.getStepExecutions().stream()
                .filter(step -> "expireStep".equals(step.getStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expireStep 이 안 돌았다"));
    }

    private LocalDateTime historyCreatedAt(long id) {
        return jdbcClient.sql("SELECT created_at FROM issuance_histories "
                        + "WHERE issuance_id = :id AND event_type = 'EXPIRE'")
                .param("id", id)
                .query(LocalDateTime.class)
                .single();
    }

    private String statusOf(long id) {
        return jdbcClient.sql("SELECT status FROM issuances WHERE id = :id")
                .param("id", id)
                .query(String.class)
                .single();
    }

    private int activeCount() {
        return jdbcClient.sql("SELECT active_count FROM coupon_stocks WHERE coupon_id = :id")
                .param("id", seed.currentCouponId())
                .query(Integer.class)
                .single();
    }

    /**
     * <b>잡이 쓰는 시각을 고정한다.</b> {@code TimeProvider} 를 통해 {@code committedAt} 이 되고,
     * 그것이 곧 이력의 {@code created_at} 이다. 고정하지 않으면 시각 단언이 벽시계에 묶인다.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class FixedClockConfig {

        @Bean
        @Primary
        Clock fixedClock() {
            // 운영 TimeConfig 가 systemUTC 다. 여기서 기본 타임존을 쓰면 CI 와 로컬이
            // 다른 값을 내고, 고정한 의미가 없어진다.
            return FixedClock.at(WROTE_AT);
        }
    }

}

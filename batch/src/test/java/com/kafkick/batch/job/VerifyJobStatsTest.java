// 통계 Step 이 잡 안에서 실제로 스냅샷을 남기고 뷰가 그것을 가리키는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.AbstractStep;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.verification.StatsRepository;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>{@code v_latest_stats_run} 을 쓰는 첫 코드가 이 티켓이다.</b> 뷰는 CY-201 이 세웠지만
 * 그때는 테스트만 읽었다 — 실제 스냅샷을 가리키는지는 확인된 적이 없다.
 *
 * <p>뷰가 존재하는 이유가 <i>"완결되지 않은 스냅샷은 물리적으로 조회되지 않는다"</i> 이므로,
 * 그 성질이 <b>잡을 한 번 돌린 뒤</b>에도 성립하는지가 이 클래스의 질문이다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2"
})
@Import({MySqlContainerConfig.class, VerifyJobStatsTest.MidRunMutationConfig.class})
class VerifyJobStatsTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job verifyJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private MidRunMutation midRunMutation;

    @Autowired
    private PlatformTransactionManager transactionManager;

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        midRunMutation.disarm();
        seed = new VerificationSeed(jdbcClient);
        seed.clear();
    }

    /** 검출이 0건인 정상 발급 하나. 이력을 붙여야 리플레이가 상태를 재구성한다. */
    private void cleanIssuance() {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(issuanceId, IssuanceEventType.ISSUE, null,
                IssuanceStatus.ISSUED, AS_OF.minusHours(1));
    }

    private long runIdOf(JobExecution execution) {
        return execution.getExecutionContext().getLong(VerifyJobConfig.RUN_ID_KEY);
    }

    private StepExecution statsStep(JobExecution execution) {
        return execution.getStepExecutions().stream()
                .filter(step -> "statsAggregateStep".equals(step.getStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("statsAggregateStep 이 안 돌았다"));
    }

    private Long latestStatsRun() {
        return jdbcClient.sql("SELECT id FROM v_latest_stats_run")
                .query(Long.class)
                .optional()
                .orElse(null);
    }

    private int countIn(String table, long runId) {
        return jdbcClient.sql("SELECT COUNT(*) FROM " + table + " WHERE run_id = :runId")
                .param("runId", runId)
                .query(Integer.class)
                .single();
    }

    /**
     * <b>세 테이블이 다 채워지고 뷰가 그 실행을 가리켜야 끝난 것이다.</b> 하나라도 비면
     * 대시보드가 절반만 있는 스냅샷을 읽는다.
     */
    @Test
    @DisplayName("CLEAN 은 세 스냅샷을 남기고 뷰가 그 실행을 가리킨다")
    void writeSnapshotsAndPointViewAtRun() throws Exception {
        cleanIssuance();
        // 발급이 없는 회차를 하나 더 둔다. 이것이 없으면 coupons 드라이빙을 issuances
        // 드라이빙으로 바꾸는 회귀를 이 테스트가 못 잡는다.
        seed.newCoupon();

        JobExecution execution = launch("CLEAN", 1);
        long runId = runIdOf(execution);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(countIn("coupon_stats", runId))
                .as("회차 둘 — 발급이 없는 회차도 행을 받는다")
                .isEqualTo(2);
        assertThat(countIn("grade_stats", runId))
                .as("등급 쌍은 존재하는 것만 — 빈 회차는 쌍이 없다")
                .isEqualTo(1);
        assertThat(countIn("hourly_stats", runId))
                .as("7 × 24. 빈 칸도 0 으로 쓴다")
                .isEqualTo(168);
        assertThat(latestStatsRun())
                .as("뷰가 이 실행을 가리켜야 대시보드가 이 스냅샷을 읽는다")
                .isEqualTo(runId);
        // batch 모듈에는 로깅 채널이 없어 Step 카운터와 종료 메시지가 유일한 관측 수단이다.
        StepExecution stats = statsStep(execution);
        assertThat(stats.getWriteCount())
                .as("회차 2 + 등급쌍 1 + 요일시각 168")
                .isEqualTo(171);
        assertThat(stats.getExitStatus().getExitDescription())
                .contains("회차 2").contains("등급쌍 1").contains("요일시각 168");
    }

    /**
     * <b>불합격 데이터 위의 집계도 뜻이 없다.</b> CORRUPT 를 건너뛴 근거가 그것인데, CLEAN 에서
     * 검출이 났다는 것은 <b>그 데이터가 실제로 어긋났다</b>는 뜻이라 같은 근거가 적용된다.
     *
     * <p>이것이 없으면 시연 직전 재고 불일치 한 건이 남은 상태로 검증을 돌릴 때, 대시보드가
     * <b>"정합성 불합격 데이터로 만든 통계"</b> 를 아무 표시 없이 보여 준다 — 그 실행의
     * {@code attempt} 가 더 크므로 뷰가 직전 합격 스냅샷 대신 그것을 가리킨다.
     */
    @Test
    @DisplayName("검출이 있어 FAIL 로 닫힌 CLEAN 실행은 통계를 건너뛴다")
    void skipStatsWhenVerdictIsFail() throws Exception {
        cleanIssuance();
        long passed = runIdOf(launch("CLEAN", 1));

        // 재고를 어긋내 V1 을 하나 울린다 → verdict = FAIL
        seed.overwriteStock(5);
        JobExecution failedExecution = launch("CLEAN", 2);
        long failed = runIdOf(failedExecution);

        assertThat(runStatsStatus(failed))
                .as("NULL 이 아니라 SKIPPED 여야 한다 — '통계 안 함' 과 '진행 중' 은 다르다")
                .isEqualTo("SKIPPED");
        assertThat(countIn("coupon_stats", failed)).isZero();
        // docs/10 이 "if 로 감추지 않는다" 를 결정으로 못 박았다. 그 결정을 지키는 것이
        // 이 단언뿐이다 — 없으면 setExitStatus 두 줄을 지워도 전부 초록이다.
        StepExecution stats = statsStep(failedExecution);
        assertThat(stats.getExitStatus().getExitCode()).isEqualTo("SKIPPED");
        assertThat(stats.getExitStatus().getExitDescription()).contains("verdict=FAIL");
        assertThat(failedExecution.getExitStatus().getExitCode())
                .as("SimpleJob 이 마지막 Step 값을 잡에 대입한다 — "
                        + "잡 종료 코드를 신호로 쓰면 안 되는 근거다")
                .isEqualTo("SKIPPED");
        assertThat(latestStatsRun())
                .as("뷰는 직전 합격 스냅샷을 유지한다")
                .isEqualTo(passed);
    }

    /**
     * <b>재실행이 스냅샷을 두 벌로 만들면 안 된다.</b> {@code (run_id, coupon_id)} 가 PK 라
     * 같은 실행에 두 번 쓰면 중복키로 죽고, 뷰는 나중 실행을 가리켜야 한다.
     */
    @Test
    @DisplayName("attempt 를 올려 다시 돌리면 뷰가 나중 실행을 가리킨다")
    void pointViewAtLaterAttempt() throws Exception {
        cleanIssuance();

        long first = runIdOf(launch("CLEAN", 1));
        long second = runIdOf(launch("CLEAN", 2));

        assertThat(countIn("hourly_stats", first))
                .as("앞 실행 스냅샷은 남는다 — run_id 로 쌓는 이유다")
                .isEqualTo(168);
        assertThat(latestStatsRun())
                .as("같은 asOf 면 나중 attempt 가 답이다")
                .isEqualTo(second);
    }

    /**
     * <b>발급건마다 {@code ISSUE} 이력이 정확히 하나여야 한다.</b> 이력이 없는 발급건은
     * {@code asof_state} 에 실리지 않아 V3·V5 의 시야 밖이고 V4 는 반대 방향(고아 이력)만 본다 —
     * 규칙 여섯 중 아무도 이것을 보지 않으므로 통계가 잡는 자리다.
     *
     * <p>잡히지 않으면 요일·시각 합계가 발급 수보다 <b>적은</b> 스냅샷이 합격 표시를 달고 뷰에
     * 걸린다. 대시보드에서는 데이터 파손이 아니라 "그 시각에 발급이 적었다" 로 보인다.
     *
     * <p><b>총합 비교로는 못 잡는다</b>는 것이 이 검사의 형태를 정한 근거다 — 이력 없는 발급건
     * 하나와 이력이 둘인 발급건 하나가 있으면 총합이 같아 통과한다.
     */
    @Test
    @DisplayName("ISSUE 이력이 없는 발급건이 있으면 통계가 실패한다")
    void failWhenIssuanceHasNoIssueHistory() throws Exception {
        cleanIssuance();
        // 이력을 붙이지 않는다. 이것이 이 테스트의 전부다.
        long orphan = seed.issuance(IssuanceStatus.ISSUED);
        // 리플레이는 이 발급건을 못 보므로 접힌 활성은 여전히 1 이다. 맞추지 않으면 V1 이
        // 울려 verdict = FAIL 이 되고, 통계는 판정 게이트에서 먼저 빠져나가 이 검사에 못 닿는다.
        seed.matchStockToReplay(1);

        JobExecution execution = launch("CLEAN", 1);

        assertThat(execution.getStatus())
                .as("구조 파손이다 — 판정 결과가 아니라 실행 실패로 끝나야 한다")
                .isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(message -> message.contains("ISSUE 이력이 없는 발급건 1건")
                        && message.contains(String.valueOf(orphan)));
        assertThat(latestStatsRun())
                .as("뷰가 가리킬 스냅샷이 없어야 한다 — stats_status 가 COMPLETE 로 안 닫힌다")
                .isNull();
    }

    /**
     * <b>지문과 스냅샷이 같은 데이터의 함수여야 한다.</b> {@code assertFrozenStep} 이
     * {@code dataset_fingerprint} 를 캡처한 뒤 이 Step 이 집계하는데, 그 사이에 원본이 움직이면
     * <b>한 행 안에서 지문과 통계가 서로 다른 데이터를 기술한다</b> — 그때 어느 쪽이 맞는지
     * 구분할 근거가 아무 데도 없다.
     *
     * <p>움직임을 <b>{@code coupon_stocks.updated_at} 하나로</b> 만든다. 내용은 그대로고
     * 시각만 바뀌는데도 얼린 스냅샷이 더 이상 그 데이터를 기술하지 않는다는 것이 요점이다.
     */
    @Test
    @DisplayName("얼린 뒤 원본이 움직이면 통계가 실패한다")
    void failWhenDatasetMovesAfterFreeze() throws Exception {
        cleanIssuance();
        // 통계 Step 직전에 재고 행을 건드린다. beforeStep 은 태스클릿 트랜잭션 밖에서 돌아
        // 커밋이 먼저 끝나므로, 태스클릿의 읽기 뷰가 이 변경을 본다.
        midRunMutation.armBefore("statsAggregateStep",
                () -> seed.overwriteStock(1, AS_OF.plusHours(1)));

        JobExecution execution = launch("CLEAN", 1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .as("움직인 축이 재고라는 것까지 메시지에 남아야 한다")
                .anyMatch(message -> message.contains("판정 뒤 통계 집계 전에 재고 축이 움직였습니다"));
        assertThat(runStatsStatus(runIdOf(execution)))
                .as("NULL 이어야 한다 — SKIPPED 는 '뜻이 없어 안 했다' 이고 이것은 사고다")
                .isNull();
    }

    /**
     * <b>집계 도중 커밋된 변경은 사전 검사가 못 본다.</b> 태스클릿의 읽기 뷰가 첫 읽기에서
     * 고정되기 때문이다(MySQL 8.0.35 에 실측). 그래서 집계 뒤 <b>새 트랜잭션</b>에서 한 번 더
     * 보는데, 이 테스트가 그 후검사만 지킨다 — 사전 검사로는 절대 초록이 안 된다.
     *
     * <p>움직이는 축을 {@code grades} 로 고른 이유가 있다. 집계는
     * {@code coupons}·{@code coupon_stocks}·{@code issuances} 를 {@code INSERT … SELECT} 로
     * 읽어 <b>공유 락을 쥐고</b> 있어서, 그 세 테이블을 밖에서 쓰려 들면 태스클릿이 커밋할 때까지
     * 막힌다. {@code grades} 는 집계가 읽지 않으므로 막히지 않고, {@code policyDigest} 의
     * 두 번째 항이 {@code grades} 의 행 수를 담아 등급 한 행만 넣어도 지문이 갈린다.
     */
    @Test
    @DisplayName("집계 도중 원본이 움직이면 새 트랜잭션 후검사가 잡는다")
    void failWhenDatasetMovesDuringAggregation() throws Exception {
        cleanIssuance();
        // 회차 집계가 끝난 뒤(등급 집계 직전) 밖에서 커밋한다. 사전 검사는 이미 지났다.
        midRunMutation.armBeforeRepositoryCall("aggregateGradeStats", this::insertGradeOutsideTx);

        JobExecution execution = launch("CLEAN", 1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .as("사전 검사가 아니라 후검사가 잡아야 한다 — 단계 이름으로 가른다. "
                        + "축은 회차 정책이다(grades 를 흔들어 policyDigest 가 갈렸다)")
                .anyMatch(message ->
                        message.contains("통계 집계 도중에 회차 정책 축이 움직였습니다"));
        assertThat(runStatsStatus(runIdOf(execution)))
                .as("COMPLETE 앞에서 죽어야 뷰가 이 스냅샷을 안 집는다")
                .isNull();
        assertThat(latestStatsRun()).isNull();
    }

    /** 태스클릿 트랜잭션 밖에서 커밋한다. 같은 트랜잭션에 붙으면 "밖에서 움직였다" 가 아니다. */
    private void insertGradeOutsideTx() {
        TransactionTemplate outside = new TransactionTemplate(transactionManager);
        outside.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        outside.executeWithoutResult(ignored -> jdbcClient.sql(
                        "INSERT INTO grades (code, bit_value) VALUES ('MID', 16)")
                .update());
    }

    private List<String> failureMessagesOf(JobExecution execution) {
        List<String> messages = new ArrayList<>();
        for (Throwable failure : execution.getAllFailureExceptions()) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                messages.add(String.valueOf(cause.getMessage()));
            }
        }
        return messages;
    }

    /** 아직 아무것도 안 닫힌 실행은 {@code NULL} 이다 — 그 자체가 구분해야 할 상태다. */
    private String runStatsStatus(long runId) {
        return jdbcClient.sql("SELECT stats_status FROM verification_runs WHERE id = :id")
                .param("id", runId)
                .query(String.class)
                .optional()
                .orElse(null);
    }

    /**
     * CLEAN 만 띄운다. {@code dataset=CORRUPT} 는 {@code rejectDatasetMismatch} 가 CLEAN 스키마
     * 위에서 거부하므로, CORRUPT 가 통계를 건너뛰는지는 이미 그 스키마에서 도는
     * {@code VerifyJobManifestTest} 가 본다 — 여기서 컨텍스트를 하나 더 띄우지 않는다.
     */
    private JobExecution launch(String dataset, int attempt) throws Exception {
        return jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", dataset)
                .addLong("attempt", (long) attempt)
                .toJobParameters());
    }

    /**
     * <b>잡이 도는 중간에 원본을 건드리는 유일한 수단이다.</b> 얼림 재확인은 "얼린 뒤 데이터가
     * 움직였다" 를 잡으므로, 밖에서 심어 두는 방식으로는 시험할 수 없다 — 잡 시작 전에 심으면
     * 그것은 그냥 초기 데이터고 얼림이 그 상태를 얼린다.
     *
     * <p>기본은 아무것도 안 한다. 무장한 테스트만 영향을 받는다.
     */
    static final class MidRunMutation implements StepExecutionListener {

        private String armedStep;
        private String armedRepositoryMethod;
        private Runnable action;

        void armBefore(String stepName, Runnable action) {
            this.armedStep = stepName;
            this.action = action;
        }

        /**
         * <b>Step 경계가 아니라 태스클릿 <i>안</i>에서 움직이게 한다.</b> "집계 도중" 은 Step
         * 시작 전이 아니라 태스클릿 트랜잭션이 열린 뒤라야 재현된다.
         */
        void armBeforeRepositoryCall(String method, Runnable action) {
            this.armedRepositoryMethod = method;
            this.action = action;
        }

        void disarm() {
            this.armedStep = null;
            this.armedRepositoryMethod = null;
            this.action = null;
        }

        @Override
        public void beforeStep(StepExecution stepExecution) {
            if (stepExecution.getStepName().equals(armedStep)) {
                fire();
            }
        }

        void onRepositoryCall(String method) {
            if (method.equals(armedRepositoryMethod)) {
                fire();
            }
        }

        private void fire() {
            Runnable armedAction = action;
            // 한 번만 돈다. 재시작·재실행으로 두 번 도는 것이 테스트 의도가 아니다.
            disarm();
            armedAction.run();
        }
    }

    /**
     * 리스너를 {@code statsAggregateStep} 에 끼운다. {@code StepBuilder} 가 만든
     * {@code TaskletStep} 은 {@link AbstractStep} 이라 만들어진 뒤에도 리스너를 붙일 수 있다 —
     * 본 코드에 테스트용 훅을 남기지 않으려고 이 방식을 쓴다.
     */
    @TestConfiguration
    static class MidRunMutationConfig {

        @Bean
        MidRunMutation midRunMutation() {
            return new MidRunMutation();
        }

        @Bean
        static BeanPostProcessor midRunMutationRegistrar(ObjectProvider<MidRunMutation> hook) {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessAfterInitialization(Object bean, String beanName) {
                    if (bean instanceof AbstractStep step) {
                        step.registerStepExecutionListener(hook.getObject());
                    }
                    if (bean instanceof StatsRepository stats) {
                        return Proxy.newProxyInstance(
                                StatsRepository.class.getClassLoader(),
                                new Class<?>[] {StatsRepository.class},
                                (proxy, method, args) -> {
                                    hook.getObject().onRepositoryCall(method.getName());
                                    try {
                                        return method.invoke(stats, args);
                                    } catch (InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                });
                    }
                    return bean;
                }
            };
        }
    }
}

// 실행이 닫히고 판정·checksum·지문이 실제로 기록되는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>여기까지 와야 검증이 판정을 낸다.</b> 이 Step 이 없던 동안 {@code verdict}·
 * {@code findings_checksum}·{@code dataset_fingerprint}·{@code finished_at} 이 전부 NULL 이었고,
 * {@code NULL = NULL} 은 UNKNOWN 이라 <b>"재실행 결과가 같은가" 를 물을 수단 자체가 없었다.</b>
 *
 * <p>그래서 <b>"잡이 COMPLETED 다" 로는 아무것도 증명되지 않는다.</b> Step 을 통째로 빼도
 * 잡은 성공한다. 저장된 행의 컬럼을 직접 읽어 확인한다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2"
})
@Import(MySqlContainerConfig.class)
class VerifyJobFinalizeTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job verifyJob;

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

    /** 정상 발급 하나. 접기·재고·사용이 전부 맞아 어느 규칙도 울지 않는다. */
    private void cleanIssuance() {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(issuanceId, IssuanceEventType.ISSUE, null,
                IssuanceStatus.ISSUED, AS_OF.minusHours(1));
    }

    /** 재고만 어긋내 V1 을 하나 울린다. */
    private void oneStockMismatch() {
        cleanIssuance();
        seed.overwriteStock(5);
    }

    private Map<String, Object> runRow(int attempt) {
        return jdbcClient.sql("""
                        SELECT verdict, finding_count, findings_checksum,
                               dataset_fingerprint, started_at, finished_at, stats_status
                          FROM verification_runs
                         WHERE attempt = :attempt
                        """)
                .param("attempt", (long) attempt)
                .query()
                .singleRow();
    }

    @Test
    @DisplayName("정상셋 0건이면 PASS 로 닫힌다 — 다섯 컬럼이 모두 채워진다")
    void closeCleanRunAsPass() throws Exception {
        cleanIssuance();

        assertThat(launch(1).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Map<String, Object> run = runRow(1);
        assertThat(run.get("verdict")).isEqualTo("PASS");
        assertThat(run.get("finding_count")).isEqualTo(0);
        assertThat(run.get("findings_checksum")).asString().hasSize(64);
        assertThat(run.get("dataset_fingerprint")).asString().hasSize(64);
        assertThat(run.get("finished_at")).isNotNull();
    }

    /**
     * <b>{@code isNotNull()} 만으로는 못 잡는다.</b> 처음에 {@code finished_at} 에
     * 잡 <b>시작</b> 시각을 넣었는데, 채워지긴 해서 테스트가 통과했다.
     * 매 실행의 소요 시간이 0 으로 기록되고 있었다.
     */
    @Test
    @DisplayName("finished_at 이 started_at 과 다르다 — 소요 시간이 0 이면 안 된다")
    void recordActualFinishTime() throws Exception {
        cleanIssuance();
        launch(1);

        Map<String, Object> run = runRow(1);
        assertThat(run.get("finished_at"))
                .as("잡 시작 시각을 그대로 쓰면 둘이 같아진다")
                .isNotEqualTo(run.get("started_at"));
    }

    /**
     * CORRUPT 는 통계를 만들지 않는다. 비워 두면 <b>"통계 진행 중" 과 "통계 안 함" 이
     * 같은 값</b>으로 보인다 — 시드도 {@code SKIPPED} 로 심는다.
     */
    @Test
    @DisplayName("CLEAN 의 통계 상태는 통계 Step 이 COMPLETE 로 닫는다")
    void completeStatsStatusForCleanRun() throws Exception {
        cleanIssuance();
        launch(1);

        assertThat(runRow(1).get("stats_status"))
                .as("판정 Step 은 CLEAN 의 이 값을 안 건드린다. 통계 Step 이 끝까지 갔을 때만 "
                        + "COMPLETE 가 되고, 그때만 v_latest_stats_run 이 이 스냅샷을 가리킨다")
                .isEqualTo("COMPLETE");
    }

    @Test
    @DisplayName("검출이 있으면 FAIL 로 닫히고 개수가 남는다")
    void closeRunWithFindingsAsFail() throws Exception {
        oneStockMismatch();

        assertThat(launch(1).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        Map<String, Object> run = runRow(1);
        assertThat(run.get("verdict")).isEqualTo("FAIL");
        assertThat(run.get("finding_count")).isEqualTo(1);
    }

    /**
     * <b>이 프로젝트가 진짜로 잡고 싶은 칸이 여기다.</b> 판정표는
     * <i>"지문 같음 + checksum 다름 → 검증기 버그"</i> 를 읽는다. 그 판정이 성립하려면
     * 같은 데이터·같은 asOf 에서 <b>두 값이 모두 같아야</b> 한다.
     */
    @Test
    @DisplayName("같은 asOf 로 다시 돌리면 checksum 과 지문이 둘 다 같다")
    void keepChecksumAndFingerprintOnRerun() throws Exception {
        oneStockMismatch();

        launch(1);
        launch(2);

        Map<String, Object> first = runRow(1);
        Map<String, Object> second = runRow(2);

        assertThat(second.get("findings_checksum"))
                .as("검출 집합이 같은데 checksum 이 다르면 검증기가 비결정적이다")
                .isEqualTo(first.get("findings_checksum"));
        assertThat(second.get("dataset_fingerprint"))
                .as("데이터가 그대로인데 지문이 다르면 지문 식이 비결정적이다")
                .isEqualTo(first.get("dataset_fingerprint"));
        assertThat(second.get("verdict")).isEqualTo(first.get("verdict"));
    }

    /**
     * 검출이 달라지면 checksum 도 달라져야 판정이 뜻을 갖는다. 지문은 <b>데이터가 바뀌었으니</b>
     * 함께 달라진다 — 그 조합이 "데이터 변경" 칸이고, 검증기 버그 칸과 구분된다.
     */
    @Test
    @DisplayName("검출이 달라지면 checksum 이 달라진다")
    void changeChecksumWhenFindingsChange() throws Exception {
        cleanIssuance();
        launch(1);

        seed.overwriteStock(5);
        launch(2);

        assertThat(runRow(2).get("findings_checksum"))
                .isNotEqualTo(runRow(1).get("findings_checksum"));
    }

    /**
     * <b>얼림이 깨진 실행에는 판정을 남기지 않는다.</b> 검출을 믿을 수 없는 실행에 판정이 붙으면
     * 나중에 그 행만 보고 합격·불합격을 읽게 된다. 판정이 비어 있는 것 자체가 신호여야 한다.
     */
    @Test
    @DisplayName("앞 Step 이 죽으면 판정이 비어 있다 — 실패한 실행에 판정을 남기지 않는다")
    void leaveVerdictEmptyWhenRunFails() throws Exception {
        cleanIssuance();

        JobExecution execution = jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CORRUPT")   // CLEAN 스키마라 시작 가드가 거부한다
                .addLong("attempt", 9L)
                .toJobParameters());

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(jdbcClient.sql("SELECT COUNT(*) FROM verification_runs WHERE verdict IS NOT NULL")
                .query(Integer.class).single())
                .as("실행 자체가 거부됐으므로 판정이 남으면 안 된다")
                .isZero();
    }

    /**
     * <b>시드가 정답을 미리 심어 둔 실행을 이어받으면 안 된다.</b>
     * {@code cy-seed/seedgen/stats.py} 가 적재할 때마다 {@code verification_runs} 를 심고
     * CORRUPT 는 정답 800행을 그 {@code run_id} 에 붙인다. 게이트가 쓰는 {@code asOf} 도
     * 그 행에서 나오므로 <b>정상 절차가 반드시 같은 키를 만난다.</b>
     *
     * <p>이어받으면 검출 수와 checksum 이 <b>배치 검출과 시드 정답의 합집합</b>이 되어,
     * 검증기가 한 건도 못 잡아도 정답이 나온다.
     */
    @Test
    @DisplayName("닫힌 실행을 이어받지 않는다 — 시드가 심은 기준 행을 검출로 세면 안 된다")
    void rejectClosedRunWithSameParameters() throws Exception {
        cleanIssuance();
        plantSeedRun(5, "FAIL");

        JobExecution execution = launch(5);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("같은 파라미터의 실행이 이미 있습니다"));
    }

    /** 시드가 심는 모양 그대로 — 판정과 종료 시각이 이미 채워진 실행. */
    private long plantSeedRun(int attempt, String verdict) {
        jdbcClient.sql("""
                        INSERT INTO verification_runs
                            (as_of, scope, dataset, attempt, verdict, stats_status,
                             finding_count, findings_checksum, dataset_fingerprint,
                             started_at, finished_at)
                        VALUES (:asOf, 'FULL', 'CLEAN', :attempt, :verdict, 'SKIPPED',
                                800, REPEAT('a', 64), REPEAT('b', 64), :asOf, :asOf)
                        """)
                .param("asOf", AS_OF)
                .param("attempt", (long) attempt)
                .param("verdict", verdict)
                .update();

        return jdbcClient.sql("SELECT id FROM verification_runs WHERE attempt = :attempt")
                .param("attempt", (long) attempt)
                .query(Long.class)
                .single();
    }

    /**
     * <b>열린 실행도 거부한다.</b> 그 상태는 앞 실행이 중간에 죽어 남은 것이고,
     * {@code asof_state} 에 부분 결과가 남아 있다. 검출이 0건이라 통과시키면
     * 새 리플레이가 못 덮은 낡은 행이 그대로 V1·V3·V5 에 섞인다.
     */
    @Test
    @DisplayName("판정이 없는 열린 실행도 거부한다 — asof_state 에 부분 결과가 남는다")
    void rejectOpenRunWithSameParameters() throws Exception {
        cleanIssuance();
        jdbcClient.sql("""
                        INSERT INTO verification_runs
                            (as_of, scope, dataset, attempt, started_at)
                        VALUES (:asOf, 'FULL', 'CLEAN', 7, :asOf)
                        """)
                .param("asOf", AS_OF)
                .update();

        JobExecution execution = launch(7);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("같은 파라미터의 실행이 이미 있습니다"));
    }

    @Test
    @DisplayName("판정이 규칙·얼림 확인 뒤이고 통계보다 앞이다")
    void runFinalizeAfterRulesAndBeforeStats() throws Exception {
        cleanIssuance();

        List<String> steps = launch(1).getStepExecutions().stream()
                .sorted(Comparator.comparingLong(StepExecution::getId))
                .map(StepExecution::getStepName)
                .toList();

        assertThat(steps).last().isEqualTo("statsAggregateStep");
        assertThat(steps.indexOf("finalizeRunStep"))
                .as("통계가 죽어도 판정은 남아야 한다")
                .isLessThan(steps.indexOf("statsAggregateStep"));
        assertThat(steps.indexOf("assertFrozenStep"))
                .isLessThan(steps.indexOf("finalizeRunStep"));
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

    private JobExecution launch(int attempt) throws Exception {
        return jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", (long) attempt)
                .toJobParameters());
    }
}

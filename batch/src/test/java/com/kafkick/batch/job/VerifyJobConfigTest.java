package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
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
 * Step 0 의 종단 확인이다. 잡을 실제로 돌려 {@code asof_state} 가 생기는지 본다.
 *
 * <p>이 테스트는 트랜잭션 밖에서 돌아 롤백이 없다. 검증 테이블은 시드가 지우고,
 * 배치 메타는 {@link JobRepositoryTestUtils} 가 지운다. 메타를 안 지우면 두 번째 테스트가
 * 같은 파라미터로 돌 때 Spring Batch 가 완료된 인스턴스라며 거부한다.
 *
 * <p>창 크기를 2 로 낮춰 창이 여러 번 넘어가는 경로를 실제로 태운다.
 * 기본값 50000 이면 어떤 테스트도 창을 한 번밖에 안 읽는다.
 *
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2"
})
@Import(MySqlContainerConfig.class)
class VerifyJobConfigTest {

    /** 실행 시작 시각(= 실제 지금)보다 앞서야 한다. asOf 는 미래를 가리킬 수 없다. */
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

    @Test
    @DisplayName("잡을 돌리면 이력이 접혀 asof_state 가 생긴다")
    void produceAsOfStateFromHistories() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(statesOf(issuanceId)).containsExactly("USED");
    }

    @Test
    @DisplayName("마지막 이력보다 앞선 asOf 는 거부한다 — asOf 는 실행 순간을 고정하는 값이지 과거 조회가 아니다")
    void rejectAsOfBeforeLatestHistory() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(1));
        used(issuanceId, AS_OF.plusHours(1));

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                message -> message.contains("마지막 이력 시각 이상이어야 합니다"));
        assertThat(asOfStateCount()).isZero();
    }

    @Test
    @DisplayName("증분 검증은 거부한다 — 받아 놓고 전수로 돌면 기록만 INCREMENTAL 로 남는다")
    void rejectIncrementalScope() throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addLocalDateTime("fromTs", AS_OF.minusMinutes(10))
                .addString("scope", "INCREMENTAL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", 1L)
                .toJobParameters();

        JobExecution execution = jobOperator.start(verifyJob, parameters);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(
                message -> message.contains("증분 검증은 아직 지원하지 않습니다"));
        assertThat(runCount()).isZero();
    }

    @Test
    @DisplayName("필수 파라미터가 빠지면 잡이 시작조차 하지 않는다 — 무엇이 빠졌는지 보여야 한다")
    void rejectMissingRequiredParameter() {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("dataset", "CLEAN")
                .addLong("attempt", 1L)
                .toJobParameters();

        assertThatThrownBy(() -> jobOperator.start(verifyJob, parameters))
                .isInstanceOf(InvalidJobParametersException.class);
    }

    @Test
    @DisplayName("창을 여러 번 넘겨도 모든 발급건이 남는다")
    void coverEveryIssuanceAcrossWindows() throws Exception {
        List<Long> issuanceIds = List.of(
                seed.issuance(IssuanceStatus.ISSUED), seed.issuance(IssuanceStatus.ISSUED),
                seed.issuance(IssuanceStatus.ISSUED), seed.issuance(IssuanceStatus.ISSUED),
                seed.issuance(IssuanceStatus.ISSUED));
        issuanceIds.forEach(id -> issued(id, AS_OF.minusHours(1)));

        launch(1);

        assertThat(asOfStateCount()).isEqualTo(issuanceIds.size());
    }

    @Test
    @DisplayName("활성 사용 건수가 채워진다 — 리플레이가 끝난 뒤 한 문장으로 센다")
    void fillActiveUsageCount() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(3));
        used(issuanceId, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);

        launch(1);

        assertThat(usageCountsOf(issuanceId)).containsExactly(1);
    }

    @Test
    @DisplayName("asOf 이전에 취소된 사용은 세지 않는다")
    void ignoreUsageCanceledBeforeAsOf() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(3));
        seed.usage(issuanceId, AS_OF.minusHours(2), AS_OF.minusHours(1));

        launch(1);

        assertThat(usageCountsOf(issuanceId)).containsExactly(0);
    }

    @Test
    @DisplayName("불법 전이가 있어도 잡은 끝까지 간다 — 기록하고 계속한다")
    void completeDespiteIllegalTransition() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.EXPIRED);
        issued(issuanceId, AS_OF.minusHours(3));
        seed.history(issuanceId, IssuanceEventType.EXPIRE,
                IssuanceStatus.USED, IssuanceStatus.EXPIRED, AS_OF.minusHours(2));

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(statesOf(issuanceId)).containsExactly("EXPIRED");
    }

    @Test
    @DisplayName("이력이 하나도 없어도 잡은 정상 종료한다")
    void completeOnEmptyDataset() throws Exception {
        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(asOfStateCount()).isZero();
    }

    @Test
    @DisplayName("attempt 를 올리면 같은 asOf 로 다시 돈다 — 결정론 증명의 전제다")
    void rerunSameAsOfWithNextAttempt() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));

        JobExecution first = launch(1);
        JobExecution second = launch(2);

        assertThat(first.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(second.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(runCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("두 번 돌리면 asof_state 전체가 바이트 단위로 같다 — 결정론의 실체다")
    void produceIdenticalAsOfStateOnRerun() throws Exception {
        long first = seed.issuance(IssuanceStatus.USED);
        issued(first, AS_OF.minusHours(3));
        used(first, AS_OF.minusHours(2));
        seed.usage(first, AS_OF.minusHours(2), null);

        long second = seed.issuance(IssuanceStatus.ISSUED);
        issued(second, AS_OF.minusHours(1));

        long runOne = launch(1).getExecutionContext().getLong("runId");
        long runTwo = launch(2).getExecutionContext().getLong("runId");

        assertThat(digestOf(runOne)).isEqualTo(digestOf(runTwo)).isNotNull();
    }

    @Test
    @DisplayName("같은 시각 이력이 섞여도 두 실행이 같다 — 타이브레이커가 빠지면 여기서 갈린다")
    void stayIdenticalWhenHistoriesShareTimestamp() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        issued(issuanceId, AS_OF.minusHours(1));
        used(issuanceId, AS_OF.minusHours(1));

        long runOne = launch(1).getExecutionContext().getLong("runId");
        long runTwo = launch(2).getExecutionContext().getLong("runId");

        assertThat(digestOf(runOne)).isEqualTo(digestOf(runTwo));
    }

    @Test
    @DisplayName("run 마다 따로 쌓인다 — asof_state 는 run 단위로 재생성한다")
    void keepStatePerRun() throws Exception {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        issued(issuanceId, AS_OF.minusHours(1));

        launch(1);
        launch(2);

        assertThat(asOfStateCount()).isEqualTo(2);
    }

    private void issued(long issuanceId, LocalDateTime at) {
        seed.history(issuanceId, IssuanceEventType.ISSUE, null, IssuanceStatus.ISSUED, at);
    }

    private void used(long issuanceId, LocalDateTime at) {
        seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, at);
    }

    private JobExecution launch(int attempt) throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", (long) attempt)
                .toJobParameters();

        return jobOperator.start(verifyJob, parameters);
    }

    private List<String> statesOf(long issuanceId) {
        return jdbcClient.sql("SELECT state FROM asof_state WHERE coupon_id = :id ORDER BY run_id")
                .param("id", issuanceId)
                .query(String.class)
                .list();
    }

    private List<Integer> usageCountsOf(long issuanceId) {
        return jdbcClient.sql(
                        "SELECT active_usage_count FROM asof_state WHERE coupon_id = :id ORDER BY run_id")
                .param("id", issuanceId)
                .query(Integer.class)
                .list();
    }

    /**
     * 상태 한 컬럼만 보면 last_history_id 가 뒤바뀌어도, 발급건 하나가 통째로 빠져도 통과한다.
     * 행 전체를 정렬해 접어야 타이브레이커 회귀가 드러난다.
     */
    private String digestOf(long runId) {
        jdbcClient.sql("SET SESSION group_concat_max_len = 1048576").update();

        return jdbcClient.sql("""
                        SELECT SHA2(GROUP_CONCAT(
                                 CONCAT_WS(0x1f, coupon_id, state,
                                           COALESCE(last_history_id, ''), last_event_at,
                                           active_usage_count)
                                 ORDER BY coupon_id SEPARATOR 0x1e), 256)
                          FROM asof_state WHERE run_id = :runId
                        """)
                .param("runId", runId)
                .query(String.class)
                .single();
    }

    private static List<String> failureMessagesOf(JobExecution execution) {
        return execution.getAllFailureExceptions().stream()
                .map(Throwable::getMessage)
                .toList();
    }

    private int asOfStateCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM asof_state").query(Integer.class).single();
    }

    private int runCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM verification_runs")
                .query(Integer.class)
                .single();
    }
}

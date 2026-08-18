// 오염셋 판정이 정답 매니페스트와의 집합 일치로 이뤄지는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.CorruptSchema;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>합격 조건이 건수에서 집합 일치로 바뀌는 자리다.</b> 이전까지 CORRUPT 는 검출 수와
 * 무관하게 상수 {@code FAIL} 이었다 — 정답 800행을 완벽히 잡아도 그 사실을 표현할 방법이 없었다.
 *
 * <p><b>CORRUPT 스키마 위에서 돈다.</b> 오염을 실제로 심어야 검출이 나오고, CLEAN 에서는
 * 제약이 그것을 물리적으로 막는다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2",
        CorruptSchema.FLYWAY_LOCATIONS
})
@Import(MySqlContainerConfig.class)
class VerifyJobManifestTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);
    private static final long SEED_RUN = 1L;

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

    /**
     * 오염 하나를 심는다 — 같은 회원이 같은 회차에서 두 번(유형 6). V2 가 잡는다.
     * 반환값은 그 검출의 {@code target_key} 다.
     */
    private String plantDuplicate() {
        long couponId = seed.currentCouponIdOrCreate();
        long memberId = seed.issuanceForNewMember();
        seed.issuanceForMember(memberId);

        return "COUPON:" + couponId + "|MEMBER:" + memberId;
    }

    /** 정답 한 건. 시드가 심는 모양 그대로. */
    private void expected(String findingType, String targetKey) {
        jdbcClient.sql("""
                        INSERT INTO expected_findings
                            (seed_run_id, corrupt_type, finding_type, target_key,
                             note, created_at)
                        VALUES (:seedRunId, 6, :findingType, :targetKey, '-', :createdAt)
                        """)
                .param("seedRunId", SEED_RUN)
                .param("findingType", findingType)
                .param("targetKey", targetKey)
                .param("createdAt", AS_OF)
                .update();
    }

    private Map<String, Object> runRow() {
        return jdbcClient.sql("""
                        SELECT verdict, finding_count, stats_status,
                               findings_checksum, dataset_fingerprint, finished_at
                          FROM verification_runs WHERE dataset = 'CORRUPT'
                        """)
                .query()
                .singleRow();
    }

    @Test
    @DisplayName("검출이 정답과 같으면 PASS 로 닫힌다 — 상수 FAIL 이 아니다")
    void passWhenFindingsMatchManifest() throws Exception {
        expected("DUP_PER_MEMBER", plantDuplicate());

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        Map<String, Object> run = runRow();
        assertThat(run.get("verdict")).isEqualTo("PASS");
        assertThat(run.get("finding_count")).isEqualTo(1);
        assertThat(run.get("stats_status")).isEqualTo("SKIPPED");
        // 오염 데이터 위의 집계는 뜻이 없다. 통계 Step 이 체인에 붙었어도 CORRUPT 는
        // 아무 스냅샷도 만들지 않고, 뷰가 COMPLETE 만 보므로 후보도 아니다.
        assertThat(jdbcClient.sql("SELECT COUNT(*) FROM hourly_stats")
                .query(Integer.class)
                .single())
                .as("CORRUPT 가 통계를 만들면 대시보드가 오염 데이터를 읽는다")
                .isZero();
        assertThat(jdbcClient.sql("SELECT COUNT(*) FROM v_latest_stats_run")
                .query(Integer.class)
                .single())
                .as("CLEAN COMPLETE 실행이 없으면 뷰는 비어 있다")
                .isZero();
        assertThat(execution.getStepExecutions())
                .filteredOn(step -> "statsAggregateStep".equals(step.getStepName()))
                .as("통계 Step 이 체인에서 빠지면 아래 단언이 조용히 사라진다")
                .singleElement()
                .satisfies(step -> {
                    assertThat(step.getExitStatus().getExitCode()).isEqualTo("SKIPPED");
                    assertThat(step.getExitStatus().getExitDescription())
                            .as("건너뛴 사실이 배치 메타에 남아야 한다 — if 로 감추지 않는다")
                            .contains("dataset=CORRUPT");
                });
        assertThat(exitMessageOf(execution))
                .contains("seedRunId=1").contains("정답 1건 / 검출 1건");
        // dataset 은 여러 실행이 공유하는 라벨이다. 이 컬럼이 존재하는 이유가
        // "묶음이 둘인 DB" 인데, 그 상황을 재현하는 순간 .single() 이 깨진다.
        long runId = execution.getExecutionContext().getLong(VerifyJobConfig.RUN_ID_KEY);
        assertThat(jdbcClient.sql("SELECT seed_run_id FROM verification_runs WHERE id = :id")
                .param("id", runId)
                .query(Long.class)
                .single())
                .as("종료 메시지는 잡 메타와 함께 지워진다. 실행 행이 남는 증적이다")
                .isEqualTo(SEED_RUN);
    }

    /**
     * <b>PASS 행만으로는 "어느 묶음과 대조했는지" 를 못 댄다.</b> {@code seedRunId} 는 잡 실행
     * 컨텍스트에만 있고 {@code verification_runs} 에는 컬럼이 없다 — 스키마 주인이 시드라
     * 이번에 늘리지 않았다.
     *
     * <p>주입을 두 번 돌려 묶음이 둘인 DB 에서 정확히 그 질문이 온다. 잡 메타를 지우는 정리
     * 배치가 한 번 돌면 컨텍스트도 사라지므로, 근거는 실행 행 쪽 메시지에 남겨야 한다.
     */
    @Test
    @DisplayName("합격도 어느 묶음과 대조했는지를 남긴다")
    void recordComparedManifestOnPass() throws Exception {
        String key = plantDuplicate();
        expected("DUP_PER_MEMBER", key);
        jdbcClient.sql("""
                        INSERT INTO expected_findings
                            (seed_run_id, corrupt_type, finding_type, target_key, note, created_at)
                        VALUES (7, 6, 'DUP_PER_MEMBER', :targetKey, '-', :createdAt)
                        """)
                .param("targetKey", key)
                .param("createdAt", AS_OF)
                .update();

        assertThat(exitMessageOf(launch(1, 7L)))
                .as("1번 묶음도 같은 검출을 갖는다 — 메시지가 없으면 둘을 구분할 수 없다")
                .contains("seedRunId=7");
    }

    /**
     * <b>개수가 같아도 집합이 다르면 통과하면 안 된다.</b> 누락 1 · 오탐 1 이라
     * {@code finding_count} 는 정답과 똑같다 — "오탐 400 + 누락 400 도 800" 이 이 모양이다.
     */
    @Test
    @DisplayName("개수가 같아도 집합이 다르면 실패한다")
    void failWhenSetsDifferDespiteSameCount() throws Exception {
        plantDuplicate();
        expected("DUP_PER_MEMBER", "COUPON:999|MEMBER:999");

        JobExecution execution = launch(1);

        assertThat(execution.getStatus())
                .as("불일치는 실행 실패가 아니라 판정 결과다")
                .isEqualTo(BatchStatus.COMPLETED);
        Map<String, Object> run = runRow();
        assertThat(run.get("verdict")).isEqualTo("FAIL");
        assertThat(exitMessageOf(execution))
                .contains("누락 1건").contains("오탐 1건").contains("정답 1건 / 검출 1건");

        assertThat(run.get("findings_checksum")).asString()
                .as("불일치여도 증적은 남는다 — 이 변경의 이유다")
                .hasSize(64);
        assertThat(run.get("dataset_fingerprint")).asString().hasSize(64);
        assertThat(run.get("finished_at")).isNotNull();
        assertThat(execution.getStepExecutions())
                .filteredOn(step -> "finalizeRunStep".equals(step.getStepName()))
                .allMatch(step -> "FAILED".equals(step.getExitStatus().getExitCode()));
    }

    @Test
    @DisplayName("못 잡은 정답이 있으면 실패한다 — 누락")
    void failWhenFindingIsMissing() throws Exception {
        expected("DUP_PER_MEMBER", plantDuplicate());
        expected("STOCK_MISMATCH", "COUPON:999");

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(runRow().get("verdict")).isEqualTo("FAIL");
        assertThat(exitMessageOf(execution))
                .contains("누락 1건").contains("STOCK_MISMATCH:COUPON:999");
    }

    /**
     * <b>오탐만 있고 누락은 없는 경우다.</b> 정답이 검출의 <b>진부분집합</b>이어야 이 상태가 된다 —
     * 처음에 정답을 상위집합으로 두었더니 누락이 함께 생겨,
     * "오탐을 무시하는" 회귀를 <b>돌연변이가 통과했다.</b>
     *
     * <p>회원 둘이 각각 두 번 받아 검출은 2건인데 정답은 그중 하나만 갖는다.
     */
    @Test
    @DisplayName("정답에 없는 것을 잡으면 실패한다 — 누락 없이 오탐만")
    void failWhenFindingIsUnexpected() throws Exception {
        String first = plantDuplicate();
        long second = seed.issuanceForNewMember();
        seed.issuanceForMember(second);
        expected("DUP_PER_MEMBER", first);

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(runRow().get("verdict")).isEqualTo("FAIL");
        assertThat(exitMessageOf(execution))
                .as("누락 0 · 오탐 1 이어야 오탐 방향만 지킨다")
                .contains("누락 0건").contains("오탐 1건");
    }

    /**
     * <b>정답 묶음이 없으면 판정하지 않는다.</b> 그대로 두면 검출 전부가 오탐으로 잡혀
     * "오탐 N건" 이라는 엉뚱한 결론이 나오고, 진짜 원인(주입을 안 돌렸다)이 안 보인다.
     */
    @Test
    @DisplayName("정답 매니페스트가 없으면 실행 전에 죽는다 — 규칙을 다 돌린 뒤가 아니다")
    void failWhenManifestIsAbsent() throws Exception {
        plantDuplicate();

        JobExecution execution = launch(1);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("정답 매니페스트가 없습니다"));
        assertThat(execution.getStepExecutions())
                .as("startRunStep 에서 죽어야 리플레이와 규칙을 안 돌린다")
                .allMatch(step -> "startRunStep".equals(step.getStepName()));
    }

    /**
     * <b>기본값을 두면 정답 묶음이 둘 이상인 DB 에서 조용히 낡은 것과 대조한다.</b>
     * 주입을 두 번 돌리면 실제로 그렇게 되고, "누락 800 · 오탐 800" 으로 나타나
     * 규칙을 의심하게 만든다.
     */
    @Test
    @DisplayName("CORRUPT 는 seedRunId 가 필수다 — 기본값이 낡은 묶음을 집는다")
    void requireSeedRunIdForCorrupt() throws Exception {
        expected("DUP_PER_MEMBER", plantDuplicate());

        JobExecution execution = launch(1, null);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("seedRunId 가 필요합니다"));
    }

    /**
     * 지정한 묶음을 실제로 본다는 것을 증명한다 — 1번 묶음에 <b>다른</b> 정답을 심어 두어야
     * "2번을 봤다" 가 성립한다. 1번만 보는 회귀는 여기서 잡힌다.
     */
    @Test
    @DisplayName("seedRunId 로 지정한 묶음과 대조한다")
    void judgeAgainstGivenSeedRun() throws Exception {
        String key = plantDuplicate();
        expected("DUP_PER_MEMBER", key);
        jdbcClient.sql("""
                        INSERT INTO expected_findings
                            (seed_run_id, corrupt_type, finding_type, target_key, note, created_at)
                        VALUES (2, 6, 'DUP_PER_MEMBER', :targetKey, '-', :createdAt)
                        """)
                .param("targetKey", key)
                .param("createdAt", AS_OF)
                .update();
        jdbcClient.sql("UPDATE expected_findings SET target_key = 'COUPON:9|MEMBER:9' "
                        + "WHERE seed_run_id = 1")
                .update();

        assertThat(launch(1, 2L).getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(runRow().get("verdict"))
                .as("1번 묶음을 봤다면 누락·오탐이 각각 1건이라 FAIL 이다")
                .isEqualTo("PASS");
    }

    /**
     * <b>재실행 축은 {@code attempt} 하나다.</b> {@code seedRunId} 를 식별 파라미터로 넣으면
     * Spring Batch 는 새 {@code JobInstance} 로 받아 잡을 <b>시작</b>하는데, DB 의
     * {@code uk_run_params(as_of, dataset, scope, attempt)} 에는 그 축이 없어
     * {@code startRunStep} 이 <i>"같은 파라미터의 실행이 이미 있습니다"</i> 로 죽인다.
     * 파라미터를 바꿔 던졌는데 그런 메시지가 나오면 원인을 못 찾는다.
     *
     * <p>비식별이면 시작 전에 <b>이미 완료된 실행</b>이라고 정확히 말한다.
     */
    @Test
    @DisplayName("seedRunId 만 바꿔 던져도 재실행 축은 attempt 다 — 시작 전에 막힌다")
    void keepAttemptAsTheOnlyRerunAxis() throws Exception {
        expected("DUP_PER_MEMBER", plantDuplicate());

        assertThat(launch(1, 1L).getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThatThrownBy(() -> launch(1, 2L))
                .as("잡을 시작해 놓고 startRunStep 에서 엉뚱한 이유로 죽으면 안 된다")
                .isInstanceOf(JobInstanceAlreadyCompleteException.class);
    }

    /**
     * <b>판정 입력도 얼려야 한다.</b> 데이터 네 축(발급건·재고·정책·이력)은
     * {@code assertFrozenStep} 이 얼리는데 매니페스트는 그 Step <b>뒤에</b> 읽힌다 —
     * 실행 중에 주입을 다시 돌리면 같은 데이터·같은 asOf 인데 <b>판정만 달라진다.</b>
     *
     * <p>실행 중 변경을 트리거로 만든다. 규칙 Step 이 검출을 쓰는 순간
     * ({@code startRunStep} 뒤, {@code finalizeRunStep} 앞) 정답이 한 건 늘어난다.
     * 가드가 없으면 그 실행은 <b>"누락 1건" 으로 조용히 FAIL</b> 하고,
     * 원인이 규칙인지 데이터가 움직인 것인지 가를 값이 하나도 없다.
     */
    @Test
    @DisplayName("실행 중에 정답 묶음이 바뀌면 판정하지 않고 거부한다")
    void rejectManifestMutatedDuringRun() throws Exception {
        expected("DUP_PER_MEMBER", plantDuplicate());
        jdbcClient.sql("""
                        CREATE TRIGGER mutate_manifest AFTER INSERT ON verification_findings
                        FOR EACH ROW INSERT INTO expected_findings
                            (seed_run_id, corrupt_type, finding_type, target_key, note, created_at)
                        VALUES (:seedRunId, 6, 'STOCK_MISMATCH',
                                CONCAT('COUPON:', NEW.id), '-', :createdAt)
                        """)
                .param("seedRunId", SEED_RUN)
                .param("createdAt", AS_OF)
                .update();

        try {
            JobExecution execution = launch(1);

            assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
            assertThat(failureMessagesOf(execution))
                    .as("'누락 1건' 이 아니라 '매니페스트가 바뀌었다' 로 죽어야 원인이 보인다")
                    .anyMatch(m -> m.contains("정답 매니페스트가 실행 중에 바뀌었습니다"));
        } finally {
            jdbcClient.sql("DROP TRIGGER IF EXISTS mutate_manifest").update();
        }
    }

    /**
     * <b>설명은 방어가 아니다.</b> 오류 메시지에 "끝의 false 가 비식별이다" 를 적어 두었지만,
     * 식별로 던져도 코드가 아무 말 없이 받으면 운영자는 그 문장을 볼 기회조차 없다 —
     * 잡이 시작돼 {@code rejectExistingRun} 의 <i>"같은 파라미터의 실행이 이미 있습니다"</i> 로
     * 죽어, 파라미터를 바꿔 던졌는데 그런 메시지를 보게 된다.
     */
    @Test
    @DisplayName("seedRunId 를 식별로 던지면 거부한다 — 메시지로 설명만 하고 막지 않으면 소용없다")
    void rejectIdentifyingSeedRunId() throws Exception {
        expected("DUP_PER_MEMBER", plantDuplicate());

        JobExecution execution = jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CORRUPT")
                .addLong("attempt", 1L)
                .addLong("seedRunId", SEED_RUN)
                .toJobParameters());

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("seedRunId 는 비식별이어야 합니다"));
        assertThat(execution.getStepExecutions())
                .as("규칙을 다 돌린 뒤가 아니라 startRunStep 에서 막아야 한다")
                .allMatch(step -> "startRunStep".equals(step.getStepName()));
    }

    private JobExecution launch(int attempt) throws Exception {
        return launch(attempt, SEED_RUN);
    }

    private JobExecution launch(int attempt, Long seedRunId) throws Exception {
        JobParametersBuilder builder = new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CORRUPT")
                .addLong("attempt", (long) attempt);
        if (seedRunId != null) {
            // 비식별이다. 식별로 넣으면 JobInstance 축이 uk_run_params(as_of, dataset, scope,
            // attempt) 와 어긋나, seedRunId 만 바꿔 던진 실행이 잡을 시작한 뒤 startRunStep 에서
            // "같은 파라미터의 실행이 이미 있습니다" 로 죽는다 — 파라미터를 바꿨는데 그렇게 나온다.
            builder.addLong("seedRunId", seedRunId, false);
        }
        return jobOperator.start(verifyJob, builder.toJobParameters());
    }

    private String exitMessageOf(JobExecution execution) {
        return execution.getStepExecutions().stream()
                .filter(step -> "finalizeRunStep".equals(step.getStepName()))
                .map(step -> step.getExitStatus().getExitDescription())
                .findFirst()
                .orElse("");
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
}

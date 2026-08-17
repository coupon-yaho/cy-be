// 통계 Step 이 잡 안에서 실제로 스냅샷을 남기고 뷰가 그것을 가리키는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;

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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
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
@Import(MySqlContainerConfig.class)
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

    private VerificationSeed seed;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
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

        JobExecution execution = launch("CLEAN", 1);
        long runId = runIdOf(execution);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(countIn("coupon_stats", runId))
                .as("회차 하나 — 발급이 없는 회차도 행을 받는다")
                .isEqualTo(1);
        assertThat(countIn("grade_stats", runId)).isEqualTo(1);
        assertThat(countIn("hourly_stats", runId))
                .as("7 × 24. 빈 칸도 0 으로 쓴다")
                .isEqualTo(168);
        assertThat(latestStatsRun())
                .as("뷰가 이 실행을 가리켜야 대시보드가 이 스냅샷을 읽는다")
                .isEqualTo(runId);
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
}

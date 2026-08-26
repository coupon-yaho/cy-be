// V2 가 잡 체인에서 실제로 돌고 상한도 걸리는지 CORRUPT 스키마 위에서 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.CorruptMySqlContainerConfig;
import com.kafkick.storage.db.CorruptSchema;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>CORRUPT 스키마 위에서 돕니다.</b> V2 가 잡는 두 케이스는 CLEAN 에서
 * {@code uk_coupon_member} 와 {@code issuances.code} UNIQUE 에 막혀 <b>심을 수조차 없습니다.</b>
 * 규칙 자체는 {@code DuplicateIssuanceRuleTest} 가 보고, 여기서는 <b>잡 배선</b>을 봅니다 —
 * 스텝이 체인에 실제로 들어갔는지, 상한이 다른 규칙과 같은 의미로 걸리는지.
 *
 * <p>상한을 1 로 낮춰 둡니다. 그래야 "정확히 상한이면 통과 · 넘으면 정지" 를 두 건으로 가릅니다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2",
        "batch.verify.max-findings-per-rule=1",
        CorruptSchema.FLYWAY_LOCATIONS
})
// CORRUPT 전용 컨테이너다. 로케이션(CorruptSchema)과 **함께** 줘야 한다 —
// 하나만 주면 조용히 틀린다. 이유는 CorruptMySqlContainerConfig 에 있다.
@Import(CorruptMySqlContainerConfig.class)
class VerifyJobDuplicateIssuanceTest {

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

    /**
     * <b>CORRUPT 실행은 정답 매니페스트가 있어야 판정된다.</b> 없으면 검출 전부가 오탐이 되어
     * 대조 자체가 성립하지 않는다. 이 클래스는 <b>배선</b>을 보므로, 시나리오가 내는 검출을
     * 그대로 정답으로 심어 판정이 통과하게 둔다.
     */
    private void expectManifest(String targetKey) {
        jdbcClient.sql("""
                        INSERT INTO expected_findings
                            (seed_run_id, corrupt_type, finding_type, target_key, note, created_at)
                        VALUES (1, 6, 'DUP_PER_MEMBER', :targetKey, '-', :createdAt)
                        """)
                .param("targetKey", targetKey)
                .param("createdAt", AS_OF)
                .update();
    }

    /** 같은 회원이 같은 회차에서 두 번 — 오염 유형 6 의 모양. */
    private long duplicateForOneMember() {
        long memberId = seed.issuanceForNewMember();
        seed.issuanceForMember(memberId);
        return memberId;
    }

    @Test
    @DisplayName("1인 1매 위반이 잡 실행에서 V2 로 남는다 — 상한 1 에 정확히 닿아도 통과한다")
    void recordDuplicatePerMember() throws Exception {
        long couponId = seed.currentCouponIdOrCreate();
        long memberId = duplicateForOneMember();
        expectManifest("COUPON:" + couponId + "|MEMBER:" + memberId);

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(findingsOf()).containsExactly(
                "DUP_PER_MEMBER:COUPON:" + couponId + "|MEMBER:" + memberId);
    }

    /**
     * <b>매니페스트가 없으면 규칙을 돌리기 전에 죽는다.</b> 검출이 한 건도 안 쌓이는 것이
     * 그 증거다 — 이전에는 규칙을 다 돌린 뒤 마지막 Step 에서 알았다.
     *
     * <p>규칙 자체의 "회원이 다르면 검출하지 않는다" 는 {@code DuplicateIssuanceRuleTest} 가 본다.
     */
    @Test
    @DisplayName("매니페스트가 없으면 규칙을 돌리기도 전에 죽는다")
    void rejectBeforeRunningRulesWhenManifestIsAbsent() throws Exception {
        seed.issuanceForNewMember();

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .anyMatch(m -> m.contains("정답 매니페스트가 없습니다"));
        assertThat(findingsOf())
                .as("규칙이 안 돌았으므로 검출이 쌓일 수 없다")
                .isEmpty();
    }

    @Test
    @DisplayName("V2 검출이 상한을 넘으면 잡을 멈춘다 — 규칙마다 상한이 따로 걸린다")
    void stopWhenDuplicatesExceedLimit() throws Exception {
        expectManifest("COUPON:0|MEMBER:0");   // 매니페스트가 있어야 규칙까지 간다
        duplicateForOneMember();
        duplicateForOneMember();

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution))
                .as("다른 규칙이 먼저 터져서 통과하면 이 테스트는 아무것도 지키지 않는다")
                .anyMatch(m -> m.contains("duplicateIssuanceStep 검출이 상한에 닿았습니다"));
    }

    private JobExecution launch() throws Exception {
        return jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CORRUPT")
                .addLong("attempt", 1L)
                // 비식별이다. 근거는 VerifyJobManifestTest#keepAttemptAsTheOnlyRerunAxis —
                // 식별로 넣으면 JobInstance 축이 uk_run_params 와 어긋나 재실행이 엉뚱한
                // 이유로 죽는다. 프로덕션 런처가 없어 이 두 테스트가 파라미터 규약이다.
                .addLong("seedRunId", 1L, false)
                .toJobParameters());
    }

    private List<String> findingsOf() {
        return jdbcClient.sql("""
                        SELECT CONCAT(finding_type, ':', target_key)
                          FROM verification_findings
                         ORDER BY finding_type, target_key
                        """)
                .query(String.class)
                .list();
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

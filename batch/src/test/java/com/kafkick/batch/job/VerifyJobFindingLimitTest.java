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

import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * 검출 상한이 실제로 잡을 멈추는지 본다. 상한을 1 로 낮춰 두 건을 심는다.
 *
 * <p>이 안전장치는 "검증기가 망가지면 위반이 수백만 건으로 튀고 그대로 담으면 OOM 으로 죽어
 * 원인이 묻힌다" 를 막으려고 둔 것이다. 발동을 증명하지 않으면 안전장치가 아니다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.verify.chunk-size=2",
        "batch.verify.replay-window-size=2",
        "batch.verify.max-findings-per-rule=1"
})
@Import(MySqlContainerConfig.class)
class VerifyJobFindingLimitTest {

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
    @DisplayName("규칙 검출이 상한을 넘으면 잡을 멈춘다")
    void stopWhenRuleFindingsExceedLimit() throws Exception {
        mismatched();
        mismatched();

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(m -> m.contains("검출이 상한에 닿았습니다"));
    }

    @Test
    @DisplayName("상한 이하면 그대로 통과한다 — 경계에서 불필요하게 죽지 않는다")
    void passAtLimit() throws Exception {
        mismatched();

        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);
    }

    @Test
    @DisplayName("V4 검출이 상한을 넘어도 잡을 멈춘다 — 모수가 가장 큰 규칙이다")
    void stopWhenIllegalTransitionsExceedLimit() throws Exception {
        illegalTransition();
        illegalTransition();

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        assertThat(failureMessagesOf(execution)).anyMatch(m -> m.contains("V4 검출이 상한에 닿았습니다"));
    }

    /** 청크 예외는 FatalStepExecutionException 으로 감싸여 원인 사슬 안에 들어간다. */
    private static List<String> failureMessagesOf(JobExecution execution) {
        List<String> messages = new ArrayList<>();
        for (Throwable failure : execution.getAllFailureExceptions()) {
            for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                messages.add(String.valueOf(cause.getMessage()));
            }
        }
        return messages;
    }

    /** 접힌 상태는 USED 인데 저장값이 ISSUED — V3 하나만 울린다. */
    private void mismatched() {
        long issuanceId = seed.issuance(IssuanceStatus.ISSUED);
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(3));
        seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.ISSUED, IssuanceStatus.USED, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);
    }

    /** 종단 상태에서 되살리는 이력 — V4 하나만 울린다. */
    private void illegalTransition() {
        long issuanceId = seed.issuance(IssuanceStatus.USED);
        seed.history(issuanceId, IssuanceEventType.ISSUE,
                null, IssuanceStatus.ISSUED, AS_OF.minusHours(4));
        seed.history(issuanceId, IssuanceEventType.EXPIRE,
                IssuanceStatus.ISSUED, IssuanceStatus.EXPIRED, AS_OF.minusHours(3));
        seed.history(issuanceId, IssuanceEventType.USE,
                IssuanceStatus.EXPIRED, IssuanceStatus.USED, AS_OF.minusHours(2));
        seed.usage(issuanceId, AS_OF.minusHours(2), null);
    }

    private JobExecution launch() throws Exception {
        return jobOperator.start(verifyJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .addString("scope", "FULL")
                .addString("dataset", "CLEAN")
                .addLong("attempt", 1L)
                .toJobParameters());
    }
}

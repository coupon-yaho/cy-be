// 배치 메타데이터가 실제로 DB 에 남는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>이 저장소가 배선되지 않으면 배치 메타데이터가 어디에도 안 남는다.</b>
 *
 * <p>Spring Batch 6 의 기본값은 {@code ResourcelessJobRepository} 이고 Boot 4 의 배치
 * 자동설정이 그것을 그대로 쓴다. 실측으로 확인했다 — 배선 전에는 이랬다.
 *
 * <pre>
 *   repoClass  = ResourcelessJobRepository
 *   instanceId = 1            ← 파라미터가 무엇이든 항상 1
 *   BATCH_JOB_INSTANCE = 0    BATCH_JOB_EXECUTION = 0    BATCH_STEP_EXECUTION = 0
 * </pre>
 *
 * <p><b>이것을 아무도 안 보고 있었다는 것이 진짜 문제였다.</b> 잡은 초록으로 끝나고,
 * {@code V2__batch_metadata.sql} 이 만든 아홉 테이블은 조용히 비어 있었다. 그 상태에서
 * 코드 주석 셋이 거짓이 된다 — 중복 방지, 실행 이력, 진도 이어받기. 이 테스트가 그 셋을
 * 각각 <b>실제 행</b>으로 확인한다.
 *
 * <p>클래스 이름만 보는 단언을 하나 남긴 이유는 진단 속도다. 나머지 단언이 깨지면
 * 원인 후보가 여럿이지만, 이것이 함께 깨지면 배선이 풀린 것으로 바로 좁혀진다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false"
})
@Import(MySqlContainerConfig.class)
class BatchJobRepositoryTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 1, 15, 9, 0);

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private Job expireJob;

    @Autowired
    private JdbcClient jdbcClient;

    @BeforeEach
    void setUp() {
        new JobRepositoryTestUtils(jobRepository).removeJobExecutions();
        new VerificationSeed(jdbcClient).clear();
    }

    @Test
    @DisplayName("실행 이력이 BATCH_* 테이블에 실제로 남는다")
    void writesMetadataToTheDatabase() throws Exception {
        assertThat(jobRepository)
                .as("기본값으로 돌아가면 아래 단언이 전부 0행으로 깨진다. "
                        + "그때 원인을 여기서 바로 읽으라고 남긴 단언이다")
                .isNotInstanceOf(ResourcelessJobRepository.class);

        JobExecution execution = launch();

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(rowCount("BATCH_JOB_INSTANCE"))
                .as("인스턴스가 남아야 같은 파라미터의 재실행을 막을 수 있다")
                .isEqualTo(1);
        assertThat(rowCount("BATCH_JOB_EXECUTION"))
                .as("**언제 돌았나가 여기 남는다.** 운영자가 볼 유일한 곳이다")
                .isEqualTo(1);
        assertThat(rowCount("BATCH_STEP_EXECUTION"))
                .as("알림 규칙이 WRITE_COUNT 로 처리량을 보라고 안내하는 그 테이블이다")
                .isEqualTo(1);
    }

    /**
     * <b>중복 방지가 <i>프로세스 밖</i>에서도 사는지 본다.</b>
     *
     * <p>예외가 나는 것만으로는 부족하다 — {@code ResourcelessJobRepository} 도 <b>같은
     * JVM 안에서는</b> 인스턴스를 기억해서 두 번째 실행을 막는다(배선을 지우고 돌려 확인했다).
     * 즉 이 예외 단언 하나는 배선이 풀린 것을 못 잡는다.
     *
     * <p>갈리는 것은 <b>그 사실이 DB 에 있느냐</b>다. 배치를 여러 대로 띄우면 서로의 메모리를
     * 못 보므로, {@code BATCH_JOB_INSTANCE} 에 행이 있어야만 중복 방지가 인스턴스 경계를
     * 넘는다. {@code ExpireScheduler} 가 {@code asOf} 를 분 단위로 자르는 근거가 그것이다.
     */
    @Test
    @DisplayName("같은 asOf 로 완료된 실행은 다시 돌지 않고, 그 사실이 DB 에 남는다")
    void refusesToRerunACompletedAsOf() throws Exception {
        assertThat(launch().getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThatThrownBy(this::launch)
                .isInstanceOf(JobInstanceAlreadyCompleteException.class);

        assertThat(rowCount("BATCH_JOB_INSTANCE"))
                .as("**막았다는 사실이 DB 에 있어야 다른 인스턴스도 그것을 본다.** "
                        + "메모리에만 있으면 배치를 두 대로 늘리는 순간 둘 다 돈다")
                .isEqualTo(1);
    }

    private JobExecution launch() throws Exception {
        return jobOperator.start(expireJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", AS_OF)
                .toJobParameters());
    }

    private int rowCount(String table) {
        return jdbcClient.sql("SELECT COUNT(*) FROM " + table).query(Integer.class).single();
    }
}

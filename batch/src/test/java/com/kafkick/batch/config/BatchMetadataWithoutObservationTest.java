package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>관측을 꺼도 잡 중복 방지가 살아 있어야 한다.</b>
 *
 * <p>{@code observation.datasource.enabled} 는 {@code application.yml.example} 이
 * <i>"유휴 시간처럼 관측이 필요 없는 구간을 위한 스위치"</i> 라고 적어 둔 <b>정상 운영 스위치</b>다.
 * 그 값에 배치 메타가 묶여 있으면, 끄는 순간 Spring Batch 가
 * {@code ResourcelessJobRepository} 로 떨어져 {@code JOB_INST_UN} 유니크 제약에 INSERT 가
 * 가지 않는다 — <b>두 노드가 같은 파라미터로 각자 잡을 시작할 수 있는 상태</b>가 되고,
 * 만료 배치라면 같은 회차를 두 번 훑는다. 예외도 로그도 없다.
 *
 * <p>그래서 관측 스위치와 배치 메타는 <b>따로 움직여야 한다.</b> 이 테스트가 그 분리를 고정한다.
 * 깨지는 방향이 조용하므로 — 잡은 정상으로 보이고 미터도 정상이다 — 여기가 유일한 그물이다.
 */
@SpringBootTest(properties = {
        "observation.datasource.enabled=false",
        "observation.domain-gauge.enabled=false",
        "spring.flyway.enabled=true"
})
@Import({ MySqlContainerConfig.class, BatchMetadataWithoutObservationTest.ProbeJob.class })
class BatchMetadataWithoutObservationTest {

    @TestConfiguration(proxyBeanMethods = false)
    static class ProbeJob {
        @Bean
        Job noObservationJob(JobRepository repository,
                @Qualifier("transactionManager") PlatformTransactionManager tx) {
            Step step = new StepBuilder("noObservationStep", repository)
                    .tasklet((contribution, context) -> RepeatStatus.FINISHED, tx)
                    .build();
            return new JobBuilder("noObservationJob", repository).start(step).build();
        }
    }

    @Autowired
    JobRepository jobRepository;
    @Autowired
    JobOperator jobOperator;
    @Autowired
    Job noObservationJob;
    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("관측을 꺼도 JobRepository 는 JDBC 다")
    void jobRepositoryStaysJdbcBacked() {
        assertThat(jobRepository.getClass().getName())
                .as("관측 스위치가 배치 메타를 끄면 잡 중복 방지가 함께 사라진다")
                .doesNotContain("Resourceless");
    }

    @Test
    @DisplayName("관측을 꺼도 실행 이력이 DB 에 남는다")
    void metadataStillPersisted() throws Exception {
        Integer before = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class);

        JobExecution execution = jobOperator.start(noObservationJob,
                new JobParametersBuilder().addString("runId", "no-obs-1").toJobParameters());

        assertThat(execution.getStatus().name()).isEqualTo("COMPLETED");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM BATCH_JOB_EXECUTION", Integer.class))
                .isEqualTo(before + 1);
    }
}

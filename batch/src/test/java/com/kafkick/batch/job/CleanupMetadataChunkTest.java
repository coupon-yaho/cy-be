// 정리 잡이 무엇을 남기고 무엇을 걷는지 확인합니다.
package com.kafkick.batch.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.batch.config.ExpireStepContext;
import com.kafkick.batch.config.RunningJobFixture;
import com.kafkick.storage.db.MySqlContainerConfig;
import com.kafkick.storage.db.VerificationSeed;

/**
 * <b>형제 클래스가 {@code metadata-chunk-size=1} 로 도는 탓에 안 밟히는 경로가 있다.</b>
 * 그 값이면 {@code executionIds}·{@code instanceIds} 가 언제나 길이 1 이라
 * {@code CleanupJdbcAdapter#deleteEach} 의 <b>합산 루프가 대입과 동치</b>가 된다 — 지워도
 * 초록이다(돌연변이로 확인했다). Connector/J 의 다중문 재작성 경로(배치 원소 &gt; 3)도
 * 한 번도 안 켜진다.
 *
 * <p>그래서 여기만 {@code metadata-chunk-size=5} 로 띄우고 <b>한 청크에 대상 다섯 + 인스턴스
 * 하나를 공유하는 실행 둘</b>을 심는다. 합산과 청크 경계가 그것으로 고정된다 — 틀리면
 * {@code WRITE_COUNT} 와 종료 설명이 조용히 틀리고 {@code CleanupRunningTooLong} 의
 * runbook 이 진도를 잘못 읽는다.
 *
 * <p><b>{@code .distinct()} 도 여기서 잡힌다 — 다만 한 번 놓쳤다.</b> 지운 수를 드라이버
 * 반환값의 <b>합</b>으로 셀 때는 같은 인스턴스에 {@code DELETE} 가 두 번 나가도 둘째가
 * 0행이라 합계가 그대로였다(돌연변이가 살아남았다). 지금은 어댑터가 <i>"고른 수 − 남은 수"</i>
 * 로 상태에서 뽑으므로 중복이 <b>고른 수</b>를 부풀려 {@code metaInstances} 가 다섯이 된다.
 *
 * <p>스케줄러는 끈다. 이 클래스가 재는 것은 발화가 아니라 잡의 동작이다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.verify.asof-state-keep-runs=2",
        "batch.cleanup.chunk-size=3",
        "batch.cleanup.abandoned-after-hours=24",
        "batch.cleanup.metadata-keep-days=10",
        // **형제와 다른 값이 이 클래스의 존재 이유다.** 5 면 배치 원소가 3 을 넘어
        // Connector/J 의 다중문 재작성 경로가 켜진다.
        "batch.cleanup.metadata-chunk-size=5"
})
@Import({MySqlContainerConfig.class, CleanupMetadataChunkTest.FixedClockConfig.class})
class CleanupMetadataChunkTest {

    private static final LocalDateTime AS_OF = LocalDateTime.of(2026, 4, 12, 5, 0);

    @TestConfiguration
    static class FixedClockConfig {
        @Bean
        @Primary
        Clock fixedClock() {
            return Clock.fixed(AS_OF.toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
        }
    }

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    @Qualifier("cleanupJob")
    private Job cleanupJob;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JdbcClient jdbcClient;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @BeforeEach
    void resetBatchMetadata() {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository);
        jobRepositoryTestUtils.removeJobExecutions();
    }

    /**
     * <b>한 청크가 대상 다섯을 함께 지운다.</b> 인스턴스 하나를 실행 둘이 공유하므로
     * {@code metaInstances} 는 넷이어야 한다 — {@code .distinct()} 를 지우면 같은 인스턴스에
     * 두 번 {@code DELETE} 가 나가고, 두 번째가 0행이라 <b>합계는 우연히 맞는다.</b>
     * 그래서 {@code WRITE_COUNT}(실행 수)와 종료 설명(인스턴스 수)을 함께 단언한다.
     */
    @Test
    @DisplayName("한 청크에 대상 다섯이 들어가도 합산과 인스턴스 중복 제거가 맞는다")
    void purgesFiveTargetsInOneChunk() throws Exception {
        long sharedInstance = -1;
        for (int i = 0; i < 5; i++) {
            RunningJobFixture old = RunningJobFixture.plant(
                    jobRepository, jdbcClient, ExpireStepContext.JOB_NAME,
                    AS_OF.minusDays(40).plusHours(i), Duration.ofHours(1), Duration.ofHours(1));
            long execution = old.executionId();
            if (i == 4) {
                // 마지막 하나만 앞의 인스턴스에 붙인다 — distinct 가 도는지 가르는 유일한 지점.
                jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET JOB_INSTANCE_ID = :inst "
                                + "WHERE JOB_EXECUTION_ID = :id")
                        .param("inst", sharedInstance).param("id", execution).update();
            } else if (i == 0) {
                sharedInstance = instanceOf(execution);
            }
            finishedAt(execution, AS_OF.minusDays(40).plusHours(i));
        }
        assertThat(oldMetaCount()).as("픽스처가 다섯 다 창 밖에 있어야 한다").isEqualTo(5);

        JobExecution execution = runCleanup();
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        StepExecution meta = execution.getStepExecutions().stream()
                .filter(step -> "purgeBatchMetadataStep".equals(step.getStepName()))
                .findFirst().orElseThrow();
        assertThat(meta.getWriteCount())
                .as("deleteEach 의 합산이 배치 원소 다섯을 다 더해야 한다 — "
                        + "chunk-size=1 인 형제 클래스에서는 대입과 동치라 안 걸린다")
                .isEqualTo(5);
        assertThat(meta.getExitStatus().getExitDescription())
                .as("인스턴스 하나를 실행 둘이 공유하므로 넷이다 — 고아 판정이 anti-join 이라 "
                        + "실행이 남은 인스턴스를 안 세는 것까지 여기서 함께 고정된다")
                .isEqualTo("metaExecutions=5 metaInstances=4");
        assertThat(meta.getCommitCount())
                .as("다섯이 한 청크에 들어갔으면 삭제 커밋은 한 번이다(+ 종료 한 번)")
                .isEqualTo(2);
        assertThat(oldMetaCount()).as("백로그가 다 빠져야 한다").isZero();
    }

    private long instanceOf(long executionId) {
        return jdbcClient.sql("SELECT JOB_INSTANCE_ID FROM BATCH_JOB_EXECUTION "
                        + "WHERE JOB_EXECUTION_ID = :id")
                .param("id", executionId).query(Long.class).single();
    }

    /** 세 시각을 함께 옮긴다 — 하나만 옮기면 대상 술어 셋 중 일부가 안 맞는다. */
    private void finishedAt(long executionId, LocalDateTime at) {
        jdbcClient.sql("UPDATE BATCH_JOB_EXECUTION SET CREATE_TIME = :at, START_TIME = :at, "
                        + "END_TIME = :at, STATUS = 'COMPLETED', EXIT_CODE = 'COMPLETED' "
                        + "WHERE JOB_EXECUTION_ID = :id")
                .param("at", at).param("id", executionId).update();
    }

    private int oldMetaCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM BATCH_JOB_EXECUTION "
                        + "WHERE END_TIME IS NOT NULL AND END_TIME < :cut")
                .param("cut", AS_OF.minusDays(10)).query(Integer.class).single();
    }

    private JobExecution runCleanup() throws Exception {
        return jobOperator.start(cleanupJob, new JobParametersBuilder()
                .addLocalDateTime("firedAt", AS_OF)
                .toJobParameters());
    }
}

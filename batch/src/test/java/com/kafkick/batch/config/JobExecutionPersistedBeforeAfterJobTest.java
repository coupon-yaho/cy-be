package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@link JobShutdownHookListener} 가 {@code afterJob} 에서 훅을 떼도 안전한 근거.</b>
 *
 * <p>리뷰가 <i>"스프링 배치는 모든 {@code afterJob} 이 끝난 뒤에야 최종 실행을 저장하므로,
 * 해제와 저장 사이에 무보호 구간이 생긴다"</i> 고 지적했다. <b>순서가 반대다.</b>
 *
 * <p>{@code AbstractJob.execute} 의 6.0.4 바이트코드는 네 갈래 모두 이 순서다 —
 *
 * <pre>
 *   setEndTime(...)
 *   jobRepository.update(execution)     ← 최종 상태를 <b>먼저</b> 저장
 *   listener.afterJob(execution)        ← 그 다음 afterJob
 *   jobRepository.update(execution)     ← afterJob 이 바꾼 것을 다시 저장
 * </pre>
 *
 * <p>그래서 훅을 떼는 시점에 DB 행은 이미 종단 상태다 — 그 뒤에 SIGTERM 이 와도
 * {@code STARTED} 로 남지 않는다.
 *
 * <p><b>바이트코드는 지금 버전의 사실이지 계약이 아니다.</b> 스프링이 순서를 바꾸면
 * 훅 해제 시점이 실제로 위험해지므로, 그 가정을 여기서 런타임으로 붙잡는다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false"
})
@Import(MySqlContainerConfig.class)
class JobExecutionPersistedBeforeAfterJobTest {

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    @Qualifier("cleanupJob")
    private Job cleanupJob;

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    void terminalStateIsPersistedBeforeAfterJobRuns() throws Exception {
        AtomicReference<String> statusSeenInAfterJob = new AtomicReference<>();
        AtomicReference<Boolean> endTimeSeenInAfterJob = new AtomicReference<>();

        ((AbstractJob) cleanupJob).registerJobExecutionListener(new JobExecutionListener() {
            @Override
            public void afterJob(JobExecution execution) {
                // 캐시가 아니라 **DB 를 직접** 본다. JobExecution 객체는 메모리에서 이미
                // 종단이라 그것으로는 저장 여부를 가릴 수 없다.
                jdbcClient.sql("""
                        SELECT STATUS, END_TIME IS NOT NULL AS ended
                          FROM BATCH_JOB_EXECUTION WHERE JOB_EXECUTION_ID = ?
                        """)
                        .param(execution.getId())
                        .query((rs, row) -> {
                            statusSeenInAfterJob.set(rs.getString("STATUS"));
                            endTimeSeenInAfterJob.set(rs.getBoolean("ended"));
                            return null;
                        })
                        .list();
            }
        });

        jobOperator.start(cleanupJob, new JobParametersBuilder()
                .addLocalDateTime("asOf", LocalDateTime.of(2026, 4, 1, 9, 0))
                .addLong("attempt", 1L)
                .toJobParameters());

        assertThat(statusSeenInAfterJob.get())
                .as("afterJob 시점에 DB 가 아직 STARTED 라면 훅을 여기서 떼면 안 됩니다")
                .isNotNull()
                .isNotEqualTo("STARTED");
        assertThat(endTimeSeenInAfterJob.get())
                .as("afterJob 시점에 END_TIME 이 없으면 실행이 안 닫힌 것입니다")
                .isTrue();
    }
}

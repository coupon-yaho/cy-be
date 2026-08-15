// 검증 잡의 배선입니다. 지금은 Step 0(이력 리플레이)까지만 있고 규칙 Step 이 뒤에 붙습니다.
package com.kafkick.batch.job;

import java.time.LocalDateTime;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.batch.replay.AsOfStateItemWriter;
import com.kafkick.batch.replay.IssuanceHistoryGroup;
import com.kafkick.batch.replay.IssuanceHistoryGroupReader;
import com.kafkick.batch.replay.ReplayProcessor;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.core.verification.replay.AsOfStateRepository;
import com.kafkick.core.verification.replay.ReplayHistoryRepository;
import com.kafkick.core.verification.replay.ReplayResult;

/**
 * <b>{@code attempt} 가 식별 파라미터에 들어 있어야 같은 {@code asOf} 로 다시 돌릴 수 있습니다.</b>
 * 없으면 Spring Batch 가 같은 파라미터 조합의 재실행을 거부하고, DB 도
 * {@code uk_run_params} 로 막습니다. 결정론 증명이 같은 {@code asOf} 를 두 번 도는 것이라
 * 이게 막히면 증명 자체가 불가능합니다.
 *
 * <p>Step 순서는 <b>실행 기록 → 리플레이 → 사용 건수</b> 입니다. {@code asof_state} 가
 * {@code verification_runs.id} 를 FK 로 물기 때문에 실행 행이 먼저 있어야 합니다.
 */
@Configuration(proxyBeanMethods = false)
public class VerifyJobConfig {

    public static final String JOB_NAME = "verifyJob";
    public static final String RUN_ID_KEY = "runId";

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public VerifyJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager
    ) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public Job verifyJob(Step startRunStep, Step replayStep, Step usageCountStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(startRunStep)
                .next(replayStep)
                .next(usageCountStep)
                .build();
    }

    /** 실행 행을 만들고 식별자를 잡 실행 컨텍스트에 심는다. 뒤 Step 이 이걸로 자기 행을 쓴다. */
    @Bean
    public Step startRunStep(VerificationRunRepository runs, TimeProvider timeProvider) {
        return new StepBuilder("startRunStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    JobParameters parameters = chunkContext.getStepContext()
                            .getStepExecution().getJobParameters();

                    VerificationRun started = runs.save(VerificationRun.start(
                            parameters.getLocalDateTime("asOf"),
                            parameters.getLocalDateTime("fromTs"),
                            ScopeType.valueOf(parameters.getString("scope")),
                            DatasetType.valueOf(parameters.getString("dataset")),
                            parameters.getLong("attempt").intValue(),
                            timeProvider.now()));

                    chunkContext.getStepContext().getStepExecution().getJobExecution()
                            .getExecutionContext().putLong(RUN_ID_KEY, started.id());

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step replayStep(
            IssuanceHistoryGroupReader replayReader,
            AsOfStateItemWriter replayWriter,
            @Value("${batch.verify.chunk-size:1000}") int chunkSize
    ) {
        return new StepBuilder("replayStep", jobRepository)
                .<IssuanceHistoryGroup, ReplayResult>chunk(chunkSize)
                .transactionManager(transactionManager)
                .reader(replayReader)
                .processor(new ReplayProcessor())
                .writer(replayWriter)
                .build();
    }

    /**
     * 발급건마다 세면 300만 번이라 집계 조인 한 문장으로 끝낸다.
     * 리플레이가 행을 모두 만든 뒤여야 하므로 Step 을 나눈다.
     */
    @Bean
    public Step usageCountStep(AsOfStateRepository asOfStates) {
        return new StepBuilder("usageCountStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution =
                            chunkContext.getStepContext().getStepExecution();

                    asOfStates.applyActiveUsageCounts(
                            requireRunId(stepExecution.getJobExecution()
                                    .getExecutionContext().get(RUN_ID_KEY)),
                            stepExecution.getJobParameters().getLocalDateTime("asOf"));

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public IssuanceHistoryGroupReader replayReader(
            ReplayHistoryRepository histories,
            @Value("#{jobParameters['asOf']}") LocalDateTime asOf,
            @Value("${batch.verify.replay-window-size:50000}") long windowSize
    ) {
        return new IssuanceHistoryGroupReader(histories, asOf, windowSize);
    }

    @Bean
    @StepScope
    public AsOfStateItemWriter replayWriter(
            AsOfStateRepository asOfStates,
            @Value("#{jobExecutionContext['" + RUN_ID_KEY + "']}") Long runId
    ) {
        return new AsOfStateItemWriter(asOfStates, requireRunId(runId));
    }

    private static long requireRunId(Object value) {
        if (value == null) {
            throw new IllegalStateException(
                    "검증 실행 식별자가 없습니다. startRunStep 이 먼저 돌아야 합니다.");
        }
        return ((Number) value).longValue();
    }
}

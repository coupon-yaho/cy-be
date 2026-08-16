// 검증 잡의 배선입니다. 지금은 Step 0(이력 리플레이)까지만 있고 규칙 Step 이 뒤에 붙습니다.
package com.kafkick.batch.job;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.support.CompositeItemWriter;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.kafkick.batch.replay.AsOfStateItemWriter;
import com.kafkick.batch.replay.IssuanceHistoryGroup;
import com.kafkick.batch.replay.IllegalTransitionItemWriter;
import com.kafkick.batch.replay.IssuanceHistoryGroupReader;
import com.kafkick.batch.replay.ReplayProcessor;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.core.verification.replay.AsOfStateRepository;
import com.kafkick.core.verification.replay.ReplayHistoryRepository;
import com.kafkick.core.verification.replay.ReplayResult;
import com.kafkick.core.verification.replay.ReplayScanRange;

/**
 * <b>{@code attempt} 가 식별 파라미터에 들어 있어야 같은 {@code asOf} 로 다시 돌릴 수 있습니다.</b>
 * 없으면 Spring Batch 가 같은 파라미터 조합의 재실행을 거부하고, DB 도
 * {@code uk_run_params} 로 막습니다. 결정론 증명이 같은 {@code asOf} 를 두 번 도는 것이라
 * 이게 막히면 증명 자체가 불가능합니다.
 *
 * <p>Step 순서는 <b>실행 기록 → 리플레이 → 사용 건수</b> 입니다. {@code asof_state} 가
 * {@code verification_runs.id} 를 FK 로 물기 때문에 실행 행이 먼저 있어야 합니다.
 *
 * <p><b>훑을 경계도 실행 기록 Step 에서 얼립니다.</b> 리더가 열리는 시점은 그보다 뒤라,
 * 거기서 재면 그 사이 생긴 발급건이 경계 밖으로 밀려 영원히 안 읽힙니다.
 */
@Configuration(proxyBeanMethods = false)
public class VerifyJobConfig {

    public static final String JOB_NAME = "verifyJob";

    static final String RUN_ID_KEY = "runId";
    static final String SCAN_MIN_KEY = "replay.scan.minIssuanceId";
    static final String SCAN_MAX_KEY = "replay.scan.maxIssuanceId";
    static final String SCAN_MAX_HISTORY_KEY = "replay.scan.maxHistoryId";
    static final String SCAN_LATEST_KEY = "replay.scan.latestCreatedAt";

    private static final String[] REQUIRED_PARAMETERS = {"asOf", "scope", "dataset", "attempt"};
    private static final String[] OPTIONAL_PARAMETERS = {"fromTs"};

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public VerifyJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager
    ) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    /**
     * 파라미터 검증을 잡에 건다. 없으면 {@code scope} 오타 하나에
     * {@code ScopeType.valueOf(null)} 로 죽어 무엇이 빠졌는지 스택트레이스로만 남는다.
     */
    @Bean
    public Job verifyJob(Step startRunStep, Step replayStep, Step usageCountStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .validator(new DefaultJobParametersValidator(
                        REQUIRED_PARAMETERS, OPTIONAL_PARAMETERS))
                .start(startRunStep)
                .next(replayStep)
                .next(usageCountStep)
                .build();
    }

    /**
     * 실행 행을 만들고, 훑을 경계를 얼려 잡 실행 컨텍스트에 심는다.
     *
     * <p>실행 시작 시각은 Spring Batch 가 이미 기록한 값을 쓴다. 여기서 {@code now()} 를 부르면
     * 검증 배치에 현재 시각 의존이 생기고, 다음 사람이 규칙에서도 부르게 된다.
     *
     * <p>실행 행은 <b>있으면 찾고 없으면 만든다.</b> 그냥 INSERT 하면 재시작 때 두 방향으로 막힌다 —
     * 이 Step 이 COMPLETED 인데 잡 컨텍스트가 아직 저장 전이면 {@code runId} 를 잃고,
     * 반대로 다시 INSERT 하면 {@code uk_run_params} 중복키에 걸린다.
     */
    @Bean
    public Step startRunStep(
            VerificationRunRepository runs,
            ReplayHistoryRepository histories
    ) {
        return new StepBuilder("startRunStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                    JobParameters parameters = stepExecution.getJobParameters();

                    LocalDateTime asOf = parameters.getLocalDateTime("asOf");
                    ScopeType scope = ScopeType.valueOf(parameters.getString("scope"));
                    DatasetType dataset = DatasetType.valueOf(parameters.getString("dataset"));
                    int attempt = parameters.getLong("attempt").intValue();

                    rejectUnsupportedScope(scope);

                    Optional<ReplayScanRange> scanRange = histories.scanRange(asOf);
                    scanRange.ifPresent(range -> rejectAsOfBeforeLatestHistory(asOf, range));

                    long runId = runs
                            .findByParams(asOf, dataset, scope, attempt)
                            .map(VerificationRun::id)
                            .orElseGet(() -> runs.save(VerificationRun.start(
                                    asOf, parameters.getLocalDateTime("fromTs"),
                                    scope, dataset, attempt,
                                    stepExecution.getJobExecution().getStartTime())).id());

                    ExecutionContext jobContext =
                            stepExecution.getJobExecution().getExecutionContext();
                    jobContext.putLong(RUN_ID_KEY, runId);
                    scanRange.ifPresent(range -> freeze(jobContext, range));

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .build();
    }

    @Bean
    public Step replayStep(
            IssuanceHistoryGroupReader replayReader,
            ItemWriter<ReplayResult> replayWriter,
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
                    StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();

                    asOfStates.applyActiveUsageCounts(
                            requireRunId(stepExecution.getJobExecution()),
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
            @Value("#{jobExecutionContext['" + SCAN_MIN_KEY + "']}") Long minIssuanceId,
            @Value("#{jobExecutionContext['" + SCAN_MAX_KEY + "']}") Long maxIssuanceId,
            @Value("#{jobExecutionContext['" + SCAN_MAX_HISTORY_KEY + "']}") Long maxHistoryId,
            @Value("#{jobExecutionContext['" + SCAN_LATEST_KEY + "']}") String latestCreatedAt,
            @Value("${batch.verify.replay-window-size:50000}") long windowSize
    ) {
        ReplayScanRange scanRange = minIssuanceId == null
                ? null
                : new ReplayScanRange(minIssuanceId, maxIssuanceId, maxHistoryId,
                        LocalDateTime.parse(latestCreatedAt));

        return new IssuanceHistoryGroupReader(histories, asOf, scanRange, windowSize);
    }

    /**
     * 접기의 산출물이 둘이라 라이터도 둘이다 — asOf 시점 상태와 V4 불법 전이.
     *
     * <p><b>같은 청크 트랜잭션에서 나간다.</b> 갈라 놓으면 이력을 다시 접어야 하고
     * 접기 구현이 두 벌이 되며, 재시작 뒤에 상태는 있는데 검출은 없는 구간이 생긴다.
     */
    @Bean
    @StepScope
    public ItemWriter<ReplayResult> replayWriter(
            AsOfStateRepository asOfStates,
            VerificationFindingRepository findings,
            @Value("#{jobExecutionContext['" + RUN_ID_KEY + "']}") Long runId
    ) {
        if (runId == null) {
            throw new IllegalStateException(
                    "검증 실행 식별자가 없습니다. startRunStep 이 먼저 돌아야 합니다.");
        }

        CompositeItemWriter<ReplayResult> writer = new CompositeItemWriter<>();
        writer.setDelegates(List.of(
                new AsOfStateItemWriter(asOfStates, runId),
                new IllegalTransitionItemWriter(findings, runId)));

        return writer;
    }

    /**
     * 증분 검증은 아직 구현되지 않았다. 받아 놓고 전수로 돌면 10분마다 534만 행을 다시 접고
     * {@code asof_state} 가 하루 4억 행 쌓이는데, 기록에는 INCREMENTAL 로 남아 티가 안 난다.
     */
    private static void rejectUnsupportedScope(ScopeType scope) {
        if (scope == ScopeType.INCREMENTAL) {
            throw new IllegalArgumentException(
                    "증분 검증은 아직 지원하지 않습니다. 전수로 실행하세요. scope=" + scope);
        }
    }

    /**
     * asOf 는 실행 순간을 고정하는 값이지 과거 조회 기능이 아니다.
     *
     * <p>과거를 넣으면 이력만 잘리고 {@code issuances.status} 는 현재값 그대로라,
     * 정상 데이터에서도 리플레이 결과와 현재 상태가 어긋나는 것이 당연해진다.
     */
    private static void rejectAsOfBeforeLatestHistory(LocalDateTime asOf, ReplayScanRange range) {
        if (range.isBefore(asOf)) {
            throw new IllegalArgumentException(
                    "asOf 는 마지막 이력 시각 이상이어야 합니다. asOf=" + asOf
                            + " 마지막 이력=" + range.latestCreatedAt());
        }
    }

    private static void freeze(ExecutionContext jobContext, ReplayScanRange range) {
        jobContext.putLong(SCAN_MIN_KEY, range.minIssuanceId());
        jobContext.putLong(SCAN_MAX_KEY, range.maxIssuanceId());
        jobContext.putLong(SCAN_MAX_HISTORY_KEY, range.maxHistoryId());
        jobContext.putString(SCAN_LATEST_KEY, range.latestCreatedAt().toString());
    }

    private static long requireRunId(JobExecution jobExecution) {
        Object value = jobExecution.getExecutionContext().get(RUN_ID_KEY);
        if (value == null) {
            throw new IllegalStateException(
                    "검증 실행 식별자가 없습니다. startRunStep 이 먼저 돌아야 합니다.");
        }

        return ((Number) value).longValue();
    }
}

// 검증 잡의 배선입니다. Step 0 리플레이(+V4) → 사용 건수 → V5 → V3. V1·V2·V6 이 뒤에 붙습니다.
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
import org.springframework.batch.infrastructure.item.ItemStreamWriter;
import org.springframework.batch.infrastructure.item.support.CompositeItemWriter;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;

import com.kafkick.batch.replay.AsOfStateItemWriter;
import com.kafkick.batch.replay.IllegalTransitionItemWriter;
import com.kafkick.batch.replay.IssuanceHistoryGroup;
import com.kafkick.batch.replay.IssuanceHistoryGroupReader;
import com.kafkick.batch.replay.ReplayProcessor;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.VerificationRuleRepository;
import com.kafkick.core.verification.VerificationRun;
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
 * <p>Step 순서는 <b>실행 기록 → 리플레이(+V4) → 사용 건수 → V5 → V3</b> 입니다. {@code asof_state} 가
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

    /** 규칙마다 반복하면 한 곳만 고치고 나머지를 놓친다. V1·V2·V6 이 오면 여섯 벌이 된다. */
    private static final String MAX_FINDINGS = "${batch.verify.max-findings-per-rule:10000}";

    private static final String[] REQUIRED_PARAMETERS = {"asOf", "scope", "dataset", "attempt"};
    private static final String[] OPTIONAL_PARAMETERS = {"fromTs"};

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final TransactionAttribute timeout;

    public VerifyJobConfig(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Value("${batch.verify.step-timeout-ms:600000}") long stepTimeoutMillis
    ) {
        if (stepTimeoutMillis < 1_000) {
            throw new IllegalArgumentException(
                    "Step 상한은 1000ms 이상이어야 합니다. 초 단위로 내림하기 때문입니다. 값="
                            + stepTimeoutMillis);
        }

        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.timeout = stepTimeout(stepTimeoutMillis);
    }

    /**
     * Step 트랜잭션의 데드라인. Spring 이 그 트랜잭션 안의 모든 {@code Statement} 에
     * {@code setQueryTimeout} 을 걸어 주므로 SELECT·UPDATE·batchUpdate 를 전부 덮는다.
     *
     * <p><b>문장 단위가 아니라 트랜잭션 단위다.</b> 청크 Step 에서는 청크 하나의 데드라인이므로
     * {@code chunk-size} 를 키우면 이 값의 의미도 같이 커진다. MySQL 힌트
     * {@code MAX_EXECUTION_TIME} 을 쓰지 않는 이유는 그것이 read-only SELECT 에만 먹어서
     * 이 잡에서 가장 무거운 문장(300만 행 UPDATE)을 못 덮기 때문이다.
     */
    private static TransactionAttribute stepTimeout(long millis) {
        DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
        attribute.setTimeout(Math.toIntExact(millis / 1_000));

        return attribute;
    }

    /**
     * 파라미터 검증을 잡에 건다. 없으면 {@code scope} 오타 하나에
     * {@code ScopeType.valueOf(null)} 로 죽어 무엇이 빠졌는지 스택트레이스로만 남는다.
     *
     * <p><b>실패한 실행을 이어 돌리지 않는다.</b> 검출 상한이 터지는 이유가 "검증기가 망가졌다" 인데,
     * 코드를 고치고 이어 돌리면 고치기 전 청크가 쓴 검출과 고친 뒤 검출이 한 {@code run_id} 안에
     * 섞인다. 판정이 어느 코드의 산출물인지 알 수 없게 된다. 다시 돌리려면 {@code attempt} 를
     * 올려 새 실행으로 간다 — 그것이 이 시스템의 재실행 축이다.
     *
     * <p>부분 재시작을 잃는 대가는 작다. Step 0 실측이 57초라 처음부터 다시 돌려도 1분이다.
     * <b>검증이 수십 분 단위로 길어지면 이 판단을 다시 해야 한다</b> — 그때는 실행마다 규칙 코드
     * 버전을 기록하고 버전이 다를 때만 거부하는 쪽이 맞다.
     */
    @Bean
    public Job verifyJob(
            Step startRunStep,
            Step replayStep,
            Step usageCountStep,
            Step usageMismatchStep,
            Step replayMismatchStep
    ) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .validator(new DefaultJobParametersValidator(
                        REQUIRED_PARAMETERS, OPTIONAL_PARAMETERS))
                .preventRestart()
                .start(startRunStep)
                .next(replayStep)
                .next(usageCountStep)
                .next(usageMismatchStep)
                .next(replayMismatchStep)
                .build();
    }

    /**
     * V5 사용 실적 정합. <b>현재 행을 읽는 규칙보다 먼저 둔다.</b>
     *
     * <p>이 규칙은 {@code asof_state} 안에서 끝나 asOf 만으로 완전히 재구성된다.
     * 결정론적인 것을 앞에 두면 뒤에서 폭주로 중단돼도 그 부분은 이미 확보된다.
     */
    @Bean
    public Step usageMismatchStep(
            VerificationRuleRepository rules,
            VerificationFindingRepository findings,
            @Value(MAX_FINDINGS) int maxFindings
    ) {
        return ruleStep("usageMismatchStep", findings, maxFindings,
                (runId, asOf, limit) -> rules.findUsageMismatches(runId, limit));
    }

    /** V3 리플레이 대조. {@code issuances.status} 라는 현재 값을 읽으므로 뒤에 둔다. */
    @Bean
    public Step replayMismatchStep(
            VerificationRuleRepository rules,
            VerificationFindingRepository findings,
            @Value(MAX_FINDINGS) int maxFindings
    ) {
        return ruleStep("replayMismatchStep", findings, maxFindings,
                (runId, asOf, limit) -> rules.findReplayMismatches(runId, asOf, limit));
    }

    /**
     * 규칙 하나를 Step 하나로 감싼다. 어긋난 것만 올라오므로 청크로 나누지 않는다 —
     * 정상셋 0건, 오염셋 규칙당 100건이다.
     *
     * <p>상한에 닿으면 멈춘다. 그만큼 나왔다는 것은 데이터가 아니라
     * <b>검증기가 망가졌다</b>는 신호이고, 그대로 담으면 OOM 으로 죽어 원인이 묻힌다.
     */
    private Step ruleStep(
            String name,
            VerificationFindingRepository findings,
            int maxFindings,
            RuleQuery query
    ) {
        // 라이터 쪽과 거부 조건을 같게 둔다. 여기서 안 막으면 어댑터까지 내려가서야 걸리고,
        // 그때 메시지에 찍히는 값이 설정한 값과 달라 원인을 못 찾는다.
        if (maxFindings < 1 || maxFindings == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "검출 상한은 1 이상 " + (Integer.MAX_VALUE - 1) + " 이하여야 합니다. 값="
                            + maxFindings);
        }

        return new StepBuilder(name, jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                    long runId = requireRunId(stepExecution.getJobExecution());
                    LocalDateTime asOf = stepExecution.getJobParameters().getLocalDateTime("asOf");

                    // 하나를 더 요청한다. LIMIT 이 상한까지만 주면 "정확히 상한" 과 "넘침" 을
                    // 구분할 수 없어, 위반이 딱 상한개인 정상 상황도 실패로 만든다.
                    List<VerificationFinding> detected = query.run(runId, asOf, maxFindings + 1);
                    if (detected.size() > maxFindings) {
                        throw new IllegalStateException(
                                name + " 검출이 상한에 닿았습니다. 검증기를 의심하십시오. 상한="
                                        + maxFindings);
                    }

                    findings.appendAll(runId, detected);
                    contribution.incrementWriteCount(detected.size());

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .transactionAttribute(timeout)
                .build();
    }

    @FunctionalInterface
    private interface RuleQuery {
        List<VerificationFinding> run(long runId, LocalDateTime asOf, int limit);
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
            ReplayHistoryRepository histories,
            VerificationRuleRepository rules
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
                    rejectIssuancesUpdatedAfterAsOf(asOf, rules.countIssuancesUpdatedAfter(asOf));

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
                .transactionAttribute(timeout)
                .build();
    }

    @Bean
    public Step replayStep(
            IssuanceHistoryGroupReader replayReader,
            ItemStreamWriter<ReplayResult> replayWriter,
            @Value("${batch.verify.chunk-size:1000}") int chunkSize
    ) {
        return new StepBuilder("replayStep", jobRepository)
                .<IssuanceHistoryGroup, ReplayResult>chunk(chunkSize)
                .transactionManager(transactionManager)
                .transactionAttribute(timeout)
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
                .transactionAttribute(timeout)
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
     *
     * <p>반환 타입이 {@link ItemStreamWriter} 인 것이 중요하다. {@code ItemWriter} 로 좁히면
     * {@code @StepScope} 프록시가 JDK 프록시가 되어 그 인터페이스만 노출하고, Spring Batch 의
     * {@code writer instanceof ItemStream} 검사가 실패해 스트림으로 등록되지 않는다.
     * {@code IllegalTransitionItemWriter} 가 누적 카운터를 갖지만 그것은 폭주 감지용이고
     * 실행마다 0 부터 세는 것이 의도라, 지금은 {@code ItemStream} 에 실을 상태가 없다.
     * 상태를 실어야 하는 라이터를 넣는 순간 이 반환 타입이 실제로 필요해진다 — 그때
     * {@code ItemWriter} 로 좁혀 두면 {@code open()} 이 영원히 안 불려
     * "일부 구간만 안 써짐" 으로 나타난다.
     */
    @Bean
    @StepScope
    public ItemStreamWriter<ReplayResult> replayWriter(
            AsOfStateRepository asOfStates,
            VerificationFindingRepository findings,
            @Value(MAX_FINDINGS) int maxFindings,
            @Value("#{jobExecutionContext['" + RUN_ID_KEY + "']}") Long runId
    ) {
        if (runId == null) {
            throw new IllegalStateException(
                    "검증 실행 식별자가 없습니다. startRunStep 이 먼저 돌아야 합니다.");
        }

        CompositeItemWriter<ReplayResult> writer = new CompositeItemWriter<>();
        writer.setDelegates(List.of(
                new AsOfStateItemWriter(asOfStates, runId),
                new IllegalTransitionItemWriter(findings, runId, maxFindings)));

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

    /**
     * 이력 축 가드와 대칭이다. 이력은 {@code asOf} 이후 것이 있으면 거부하는데,
     * 발급건 축에도 같은 것이 필요하다.
     *
     * <p>없으면 V3 가 그 행들을 조용히 빼고 0건으로 통과한다 — <b>"아무 문제 없었다" 와
     * "볼 것이 안 남았다" 가 구분되지 않는다.</b> 오염셋에서는 주입기가 {@code updated_at} 을
     * 주입 시각으로 찍는 순간 기대 100건이 0건이 되고, 실패 메시지는 "누락 100" 뿐이다.
     */
    private static void rejectIssuancesUpdatedAfterAsOf(LocalDateTime asOf, long updatedAfter) {
        if (updatedAfter > 0) {
            throw new IllegalArgumentException(
                    "asOf 이후에 갱신된 발급건이 있습니다. 런타임과 스케줄러를 멈추고 다시 실행하십시오. "
                            + "asOf=" + asOf + " 갱신된 발급건=" + updatedAfter);
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

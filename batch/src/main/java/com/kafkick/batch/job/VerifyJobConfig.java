// 검증 잡의 배선입니다. Step 0 리플레이(+V4) → 사용 건수 → V5 → V2 → V3 → V1 → V6.
package com.kafkick.batch.job;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;
import java.util.Optional;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator;
import org.springframework.batch.core.job.parameters.JobParameter;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepContribution;
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
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ExpectedFindingRepository;
import com.kafkick.core.verification.FindingKey;
import com.kafkick.core.verification.FindingType;
import com.kafkick.core.verification.HourlyIssued;
import com.kafkick.core.verification.ScopeType;
import com.kafkick.core.verification.StatsRepository;
import com.kafkick.core.verification.StatsStatus;
import com.kafkick.core.verification.VerdictType;
import com.kafkick.core.verification.VerificationFinding;
import com.kafkick.core.verification.VerificationFindingRepository;
import com.kafkick.core.verification.VerificationRuleRepository;
import com.kafkick.core.verification.VerificationRun;
import com.kafkick.core.verification.VerificationRunRepository;
import com.kafkick.core.verification.exception.VerificationErrorCode;
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
 * <p><b>Step 순서의 주인은 아래 {@code verifyJob} 의 체인입니다</b> — 여기에 목록을 또 적으면
 * 갈라집니다. 순서 규율은 {@code FindingType.isDeterministic()} 이고
 * {@code VerifyJobWiringTest} 가 그것을 강제합니다. {@code asof_state} 가
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

    /** 시작에 얼린 회차 정책 지문. coupons 에 updated_at 이 없어 값을 접어 비교한다. */
    static final String POLICY_DIGEST_KEY = "policy.digest";

    static final String FINGERPRINT_KEY = "dataset.fingerprint";

    static final String SEED_RUN_ID_KEY = "manifest.seedRunId";

    /** 시작 시점의 정답 묶음 지문. 판정 입력도 데이터 네 축처럼 얼려야 한다. */
    static final String MANIFEST_DIGEST_KEY = "manifest.digest";

    /** 규칙마다 반복하면 한 곳만 고치고 나머지를 놓친다. 지금 여섯 벌이다. */
    private static final String MAX_FINDINGS = "${batch.verify.max-findings-per-rule:10000}";

    /**
     * <b>규칙 Step 의 정의다.</b> 이름 규칙으로 추측하지 않는다 — 접미사가 다른 규칙이 들어오면
     * 검사에서 조용히 빠지고, 순서 위반이 묻힌다.
     *
     * <p>{@code ruleStep} 이 진입부에서 이 표를 확인하므로 <b>표를 안 채운 규칙 Step 은
     * 컨텍스트 기동에서 죽는다.</b> {@code VerifyJobWiringTest} 가 이 표로 순서를 강제한다.
     */
    static final Map<String, FindingType> RULE_STEP_TYPES = Map.of(
            "usageMismatchStep", FindingType.USAGE_MISMATCH,
            "duplicateIssuanceStep", FindingType.DUP_PER_MEMBER,
            "replayMismatchStep", FindingType.REPLAY_MISMATCH,
            "stockMismatchStep", FindingType.STOCK_MISMATCH,
            "gradeViolationStep", FindingType.GRADE_VIOLATION);

    private static final String[] REQUIRED_PARAMETERS = {"asOf", "scope", "dataset", "attempt"};

    /**
     * {@code seedRunId} 는 CLEAN 에는 필요 없다. <b>CORRUPT 는 반드시 명시해야 하고</b>
     * 없으면 {@code startRunStep} 이 즉시 거부한다 — {@link #freezeSeedRunId} 가 이유를 적는다.
     * "선택" 인 것은 데이터셋에 따라 갈리기 때문이지 기본값이 있어서가 아니다.
     */
    private static final String[] OPTIONAL_PARAMETERS = {"fromTs", "seedRunId"};

    /** Step 종료 메시지에 실을 불일치 표본 수. batch 모듈에는 로깅 채널이 없다. */
    private static final int SAMPLE_SIZE = 10;

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
     * <p><b>문장 단위가 아니라 트랜잭션 단위다.</b> 청크 Step 에서는 청크 하나의 데드라인이다.
     * 가장 무거운 청크는 창을 새로 읽는 청크이므로, {@code chunk-size} 보다
     * <b>{@code replay-window-size}</b> 가 이 값을 좌우한다 — 창 하나가 한 트랜잭션에 통째로 올라온다. MySQL 힌트
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
            Step duplicateIssuanceStep,
            Step replayMismatchStep,
            Step stockMismatchStep,
            Step gradeViolationStep,
            Step assertFrozenStep,
            Step finalizeRunStep,
            Step statsAggregateStep
    ) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .validator(new DefaultJobParametersValidator(
                        REQUIRED_PARAMETERS, OPTIONAL_PARAMETERS))
                .preventRestart()
                .start(startRunStep)
                .next(replayStep)
                .next(usageCountStep)
                .next(usageMismatchStep)
                .next(duplicateIssuanceStep)
                .next(replayMismatchStep)
                .next(stockMismatchStep)
                .next(gradeViolationStep)
                .next(assertFrozenStep)
                .next(finalizeRunStep)
                .next(statsAggregateStep)
                .build();
    }

    /**
     * 실행이 도는 동안 <b>세 축이 모두 얼어 있었는지</b> 끝에서 다시 본다 —
     * 발급건 · 재고 · 회차 정책.
     *
     * <p>시작에만 보면 그 뒤 몇 분(리플레이 실측 57초 + 집계 + 규칙)이 무방비다.
     * 현재 행을 읽는 규칙(V3·V1·V6)이 그 구간에 갱신된 행을 조용히 빼고 0건으로 통과하는데,
     * 같은 asOf 를 스케줄러가 멈춘 상태로 다시 돌리면 검출이 나온다 — 같은 파라미터가 다른 답을 낸다.
     *
     * <p><b>축은 넷이다</b> — 발급건 · 재고 · 회차 정책 · 이력. 이력 축은 리플레이가 얼린
     * 상한과 지문이 다시 재는 값이 갈리는 것을 막는다.
     *
     * <p><b>지문도 여기서 캡처한다.</b> 같은 트랜잭션이라 검사와 지문이 같은 스냅샷을 보고,
     * {@code finalizeRunStep} 이 {@code FINGERPRINT_KEY} 로 꺼내 쓴다 —
     * 이 줄을 지우면 판정 Step 이 "데이터셋 지문이 없습니다" 로 죽는다.
     */
    @Bean
    public Step assertFrozenStep(VerificationRuleRepository rules) {
        return new StepBuilder("assertFrozenStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    LocalDateTime asOf = chunkContext.getStepContext().getStepExecution()
                            .getJobParameters().getLocalDateTime("asOf");

                    if (rules.hasIssuancesUpdatedAfter(asOf)) {
                        throw new BusinessException(
                                VerificationErrorCode.DATASET_MUTATED_DURING_RUN,
                                "실행 중에 발급건이 갱신됐습니다. issuances.updated_at <= asOf 로 자르는 "
                                        + "규칙(V2·V3·V6)이 그만큼을 비교에서 뺐으므로 "
                                        + "이 실행의 검출은 신뢰할 수 없습니다. asOf=" + asOf);
                    }

                    if (rules.hasStocksUpdatedAfter(asOf)) {
                        throw new BusinessException(
                                VerificationErrorCode.DATASET_MUTATED_DURING_RUN,
                                "실행 중에 재고가 갱신됐습니다. V1 이 그만큼을 비교에서 뺐으므로 "
                                        + "이 실행의 검출은 신뢰할 수 없습니다. asOf=" + asOf);
                    }

                    // 회차 축은 시각으로 못 본다. 시작에 얼린 지문과 대조한다 —
                    // 마스크가 바뀌면 검출은 달라지는데 dataset_fingerprint 는 같게 나와서,
                    // 판정이 "데이터는 그대로인데 검증기가 비결정적" 으로 잘못 읽는다.
                    String frozenPolicy = chunkContext.getStepContext().getStepExecution()
                            .getJobExecution().getExecutionContext().getString(POLICY_DIGEST_KEY);

                    if (!frozenPolicy.equals(rules.policyDigest())) {
                        throw new BusinessException(
                                VerificationErrorCode.DATASET_MUTATED_DURING_RUN,
                                "실행 중에 회차 정책 또는 등급 체계가 바뀌었습니다. V1·V6 이 그 사이 값을 읽었으므로 "
                                        + "이 실행의 검출은 신뢰할 수 없습니다. asOf=" + asOf);
                    }

                    // 이력 축. 리플레이는 얼린 상한까지만 읽는데 지문은 다시 재므로,
                    // 그 사이 백데이트 이력이 들어오면 같은 지문에 다른 검출이 나온다.
                    // 창이 없던 실행(asOf 이하 이력이 0건)도 검사한다. 0 을 넘기면
                    // "id > 0 이면서 asOf 이하인 행이 생겼는가" 가 되어 뜻이 정확히 맞는다 —
                    // 건너뛰면 가드가 막으려던 상황에서 정확히 꺼진다.
                    long frozenMaxHistory = chunkContext.getStepContext().getStepExecution()
                            .getJobExecution().getExecutionContext()
                            .getLong(SCAN_MAX_HISTORY_KEY, 0L);

                    if (rules.hasHistoriesAddedAbove(frozenMaxHistory, asOf)) {
                        throw new BusinessException(
                                VerificationErrorCode.DATASET_MUTATED_DURING_RUN,
                                "실행 중에 asOf 이하 이력이 추가됐습니다. 리플레이는 얼린 상한까지만 "
                                        + "읽었는데 지문은 그 행을 봅니다 — 같은 지문에 다른 검출이 "
                                        + "나옵니다. asOf=" + asOf);
                    }

                    // 지문도 여기서 읽는다. finalize 에서 읽으면 그 사이 발급 한 건에 지문이
                    // 움직여, 같은 asOf 재실행이 "데이터가 바뀜" 으로 잘못 읽힌다.
                    //
                    // 규칙이 본 데이터와 지문이 같다는 것은 트랜잭션 스냅샷이 보장하지 않는다 —
                    // 규칙 Step 은 각자 자기 트랜잭션에서 이미 돌았다. 등식을 지키는 것은
                    // 바로 위 네 가드다. 가드를 빼면 이 등식이 깨진다.
                    chunkContext.getStepContext().getStepExecution().getJobExecution()
                            .getExecutionContext()
                            .putString(FINGERPRINT_KEY, rules.datasetFingerprint(asOf));

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .transactionAttribute(timeout)
                .build();
    }

    /**
     * <b>실행을 닫는다.</b> 이것이 없으면 {@code verdict}·{@code findings_checksum}·
     * {@code dataset_fingerprint}·{@code finished_at} 이 영원히 NULL 로 남고,
     * {@code NULL = NULL} 은 UNKNOWN 이라 <b>"재실행 결과가 같은가" 를 물을 수단 자체가 없다.</b>
     * 규칙을 여섯 개 다 붙여도 판정이 안 나오는 상태가 된다.
     *
     * <p><b>{@code assertFrozenStep} 보다 뒤다.</b> 얼림이 깨진 실행은 검출을 믿을 수 없는데
     * 거기에 판정을 남기면, 나중에 그 행만 보고 합격·불합격을 읽게 된다.
     * 앞 Step 이 죽으면 이 Step 이 안 돌아 <b>판정이 비어 있는 것 자체가 신호</b>가 된다.
     *
     * <p><b>판정은 데이터셋마다 다르다.</b> CLEAN 은 검출 0건이 통과다({@link #verdictOfClean}).
     * CORRUPT 는 {@link #judgeAgainstManifest} 가 {@code expected_findings} 와 양방향
     * 집합 차를 내 <b>누락 0 · 오탐 0 일 때만</b> PASS 다 — 개수만 보면 오탐 400 + 누락 400 도
     * 800 이라 통과한다.
     *
     * <p><b>불일치도 판정이다.</b> {@code verdict=FAIL} 과 함께 checksum·지문·종료 시각을
     * 모두 남긴다 — 그 값들이 가장 필요한 순간이 여기다. <b>게이트가 읽는 것은 그
     * {@code verdict} 다</b>({@code V1__init_schema.sql} 의 컬럼 주석). Step 종료 코드
     * {@code FAILED} 는 배치 메타DB에 남는 표식일 뿐 <b>프로세스 종료코드가 아니다</b> —
     * 근거는 {@link #judgeAgainstManifest} 에 적었다.
     *
     * <p>매니페스트 부재는 여기까지 오지 않는다 — {@code startRunStep} 이 실행 전에 죽인다.
     */
    @Bean
    public Step finalizeRunStep(
            VerificationRunRepository runs,
            VerificationFindingRepository findings,
            ExpectedFindingRepository expected
    ) {
        return new StepBuilder("finalizeRunStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                    JobExecution jobExecution = stepExecution.getJobExecution();
                    DatasetType dataset = DatasetType.valueOf(
                            jobExecution.getJobParameters().getString("dataset"));

                    long runId = requireRunId(jobExecution);
                    VerificationRun run = runs.findById(runId)
                            .orElseThrow(() -> new BusinessException(
                                    VerificationErrorCode.RUN_ROW_VANISHED,
                                    "실행 행이 사라졌습니다. runId=" + runId));

                    // 합격 판정은 전수 실행만 진다. 지금은 startRunStep 이 INCREMENTAL 을
                    // 거부해 도달 불가지만, 증분 티켓이 그 가드를 푸는 순간 여기서 죽어야
                    // 판정 규칙을 함께 정하게 된다 — 지금은 그 신호가 코드 어디에도 없다.
                    if (!run.decidesVerdict()) {
                        throw new IllegalStateException(
                                "합격 판정은 전수 실행에서만 냅니다. 증분 판정 규칙이 정해지기 전까지 "
                                        + "이 경로는 열리면 안 됩니다. runId=" + runId
                                        + " scope=" + run.scope());
                    }

                    int detected = findings.countOf(runId);
                    VerdictType verdict = dataset == DatasetType.CLEAN
                            ? verdictOfClean(detected)
                            : judgeAgainstManifest(runId, frozenSeedRunId(jobExecution),
                                    detected, expected, contribution);

                    VerificationRun closed = run.finish(
                            verdict,
                            detected,
                            findings.checksumOf(runId),
                            frozenFingerprint(jobExecution),
                            // 판정 Step 이 시작한 시각을 종료로 쓴다. 잡이 아직 안 끝나
                            // getEndTime() 은 null 이고, 검증 배치는 now() 를 금지한다.
                            // 이 뒤에 남은 것은 UPDATE 한 건뿐이라 차이가 작다.
                            stepExecution.getStartTime());

                    runs.update(closed);

                    // CORRUPT 는 통계를 만들지 않는다. 비워 두면 "통계 진행 중" 과
                    // "통계 안 함" 이 같은 값으로 보인다. CLEAN 은 통계 Step 몫이라 안 건드린다.
                    //
                    // 부분 갱신을 쓴다 — update() 는 여섯 컬럼을 전부 덮으므로,
                    // 통계 갱신에 그것을 쓰면 방금 쓴 판정이 함께 지워진다.
                    if (dataset != DatasetType.CLEAN) {
                        // 판정과 같은 트랜잭션에 남긴다. startRunStep 에서 쓰면
                        // "seed_run_id 는 있는데 verdict 는 NULL" 인 행이 정상적으로 생겨,
                        // 대조가 실제로 일어났는지를 그 행만 보고는 알 수 없다 —
                        // 증적 컬럼이 "대조했다" 대신 "대조할 예정이었다" 를 담게 된다.
                        runs.recordComparedManifest(runId, frozenSeedRunId(jobExecution));
                        runs.updateStatsStatus(runId, StatsStatus.SKIPPED);
                    }

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .transactionAttribute(timeout)
                .build();
    }

    /**
     * <b>판정 뒤에 온다.</b> 통계가 죽어도 {@code verdict} 는 이미 남고, {@code stats_status} 가
     * {@code NULL} 이라 {@code v_latest_stats_run} 이 그 스냅샷을 <b>물리적으로 안 보여 준다</b> —
     * 부분값을 섞어 읽는 사고가 그 자리에서 막힌다. 반대 순서면 통계 문제로 판정을 잃는다.
     *
     * <p><b>{@code PARTIAL} 은 쓰지 않는다.</b> 중간에 죽으면 {@code stats_status} 가 손대지 않은
     * {@code NULL} 로 남고 뷰가 이미 걸러 낸다. 세 값 중 하나를 안 쓰는 것이 이상해 보이지만,
     * "절반 쓰다 죽었다" 를 표현하려고 죽는 코드가 값을 쓸 수는 없다 — 그건 트랜잭션이 할 일이다.
     *
     * <p><b>CLEAN 전용이다.</b> 오염 데이터 위의 집계는 뜻이 없고 CORRUPT 는
     * {@code finalizeRunStep} 이 {@code SKIPPED} 로 닫는다.
     *
     * <p><b>잡 종료 코드가 이 Step 값으로 덮인다.</b> {@code SimpleJob} 은 마지막 Step 의
     * {@code ExitStatus} 를 <b>대입</b>하므로({@code javap -c SimpleJob}) 불일치 때
     * {@code finalizeRunStep} 이 세운 {@code FAILED} 표식이 잡 수준에서 사라진다. 게이트가 읽는
     * 것은 {@code verification_runs.verdict} 라 판정에는 영향이 없다 — 그 표식을 신호로 쓰지
     * 말라고 {@link #judgeAgainstManifest} 에 적어 둔 이유가 이것이다.
     */
    @Bean
    public Step statsAggregateStep(StatsRepository stats, VerificationRunRepository runs) {
        return new StepBuilder("statsAggregateStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                    JobExecution jobExecution = stepExecution.getJobExecution();
                    DatasetType dataset = DatasetType.valueOf(
                            jobExecution.getJobParameters().getString("dataset"));
                    if (dataset != DatasetType.CLEAN) {
                        return RepeatStatus.FINISHED;
                    }

                    long runId = requireRunId(jobExecution);
                    LocalDateTime asOf = jobExecution.getJobParameters()
                            .getLocalDateTime("asOf");

                    // 재시작하면 앞 실행의 부분 결과가 남아 있다. 그 위에 다시 쓰면
                    // (run_id, coupon_id) 중복키로 죽거나 낡은 행이 섞인다.
                    stats.clear(runId);
                    stats.aggregateCouponStats(runId);
                    stats.aggregateGradeStats(runId);

                    // 창은 리플레이와 같다 — 얼린 상한을 넘겨야 백데이트 이력이 통계에만
                    // 들어오는 것을 막는다. 168행을 전부 쓴다: 없는 행과 0인 행은 다르다.
                    long frozenMaxHistory = jobExecution.getExecutionContext()
                            .getLong(SCAN_MAX_HISTORY_KEY, 0L);
                    stats.appendHourlyStats(runId, HourlyIssued.fillAll(
                            stats.issuedByHour(frozenMaxHistory, asOf)));

                    // 마지막이다. 여기까지 왔을 때만 뷰가 이 스냅샷을 가리킨다.
                    runs.updateStatsStatus(runId, StatsStatus.COMPLETE);

                    return RepeatStatus.FINISHED;
                }, transactionManager)
                .transactionAttribute(timeout)
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
     * V1 재고 정합. <b>현재 {@code coupon_stocks.active_count} 를 읽는다.</b>
     *
     * <p>V3 와 같은 이유로 뒤쪽에 둔다 — 현재 행에 의존하는 규칙은 결정론적인 것들 뒤다.
     * 다만 축이 다르다. V3 는 발급건 축이고 이쪽은 재고 축이라 가드도 따로 필요하다.
     *
     * <p><b>이 Step 의 상한은 도달하지 않는다.</b> {@code uk_run_finding} 때문에 회차당 한 행이
     * 최대라 <b>검출 수의 천장이 {@code coupons} 총 행수</b>이고, 그것은
     * 과거(브랜드 12 × 개월) + 현재 3 이다 — <b>CLEAN 147 · CORRUPT 291</b>
     * ({@code seedgen/config.py} 의 {@code PAST_MONTHS_CORRUPT = 24} 와 {@code CURRENT_COUPONS} 3건,
     * {@code corrupt.py} 첫머리가 "24개월 달력(291회차)" 이라고 적는다). 기본값은 10000 이다.
     *
     * <p>폭주는 상한이 아니라 {@code uk_run_finding} 중복키로 나타난다 —
     * V1 이 회차당 1행을 넘기면 그것이 신호다.
     */
    @Bean
    public Step stockMismatchStep(
            VerificationRuleRepository rules,
            VerificationFindingRepository findings,
            @Value(MAX_FINDINGS) int maxFindings
    ) {
        return ruleStep("stockMismatchStep", findings, maxFindings,
                (runId, asOf, limit) -> rules.findStockMismatches(runId, asOf, limit));
    }

    /**
     * V6 등급 자격. <b>{@code asof_state} 를 읽지 않지만 결정론은 아니다.</b>
     *
     * <p>{@code issued_grade} 는 스냅샷이라 안 변하는데 {@code coupons.eligible_grades_mask} 는
     * 살아 있는 행이다. {@code coupons} 에 {@code updated_at} 이 없어 시각으로는 못 막고,
     * 지문 재료에도 그 축이 없다 — 마스크가 바뀌면 <b>지문은 같은데 검출만 달라진다.</b>
     * 그래서 현재 행을 읽는 규칙들과 같은 구간(뒤쪽)에 두고, 정책 축은 지문으로 얼린다.
     *
     * <p>{@code runId} 는 쓰지 않지만 시그니처는 다른 규칙과 같게 둔다 —
     * 규칙마다 다른 모양이면 배선에서 하나만 어긋나도 알아채기 어렵다.
     */
    @Bean
    public Step gradeViolationStep(
            VerificationRuleRepository rules,
            VerificationFindingRepository findings,
            @Value(MAX_FINDINGS) int maxFindings
    ) {
        return ruleStep("gradeViolationStep", findings, maxFindings,
                (runId, asOf, limit) -> rules.findGradeViolations(asOf, limit));
    }

    /**
     * V2 1인 1매 위반. <b>결정론이다</b> — {@code issuances} 만 읽고 그 테이블에는
     * {@code updated_at} 이 있어 경계를 자를 수 있다. 가드도 {@code hasIssuancesUpdatedAfter}
     * 가 이미 갖고 있어 새로 만들 것이 없다.
     *
     * <p>그래서 <b>얼어 있는 축을 읽는 앞 구간</b>에 둔다 — 현재 행을 읽는 V1·V6 과 달리
     * 이 규칙은 시각으로 완전히 고정된다.
     *
     * <p><b>정상셋에서는 검출이 나올 수 없다.</b> {@code uk_coupon_member} 와
     * {@code issuances.code} UNIQUE 가 두 케이스를 물리적으로 막는다. 그것이 정상이고,
     * 이 Step 이 의미를 갖는 것은 그 제약이 없는 CORRUPT 스키마에서다.
     */
    @Bean
    public Step duplicateIssuanceStep(
            VerificationRuleRepository rules,
            VerificationFindingRepository findings,
            @Value(MAX_FINDINGS) int maxFindings
    ) {
        return ruleStep("duplicateIssuanceStep", findings, maxFindings,
                (runId, asOf, limit) -> rules.findDuplicateIssuances(asOf, limit));
    }

    /**
     * 규칙 하나를 Step 하나로 감싼다. 어긋난 것만 올라오므로 청크로 나누지 않는다 —
     * 정상셋 0건, 오염셋 규칙당 최대 200건(합계 800)이라 산출이 작다. 분포는 docs/contract.json 이 원본이다.
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
        if (!RULE_STEP_TYPES.containsKey(name)) {
            throw new IllegalStateException(
                    name + " 이 RULE_STEP_TYPES 에 없습니다. 규칙 Step 은 표에 등록해야 "
                            + "VerifyJobWiringTest 의 순서 검사가 그것을 봅니다.");
        }

        // 여기만 상한에 +1 을 하므로 여기만 Integer.MAX_VALUE 를 막는다.
        // 라이터(IllegalTransitionItemWriter)는 +1 이 없어 하한만 본다.
        // 안 막으면 넘친 음수가 어댑터까지 내려가서야 걸리고, 그때 메시지의 값이 설정한 값과 다르다.
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
     * <p>실행 행은 있으면 찾고 없으면 만든다. 다만 <b>닫힌 실행은 이어받지 않는다.</b>
     *
     * <p><b>시드가 같은 키를 미리 점유한다.</b> {@code cy-seed/seedgen/stats.py} 가 적재할 때마다
     * {@code verification_runs} 를 심고, CORRUPT 는 <b>정답 800행을 그 {@code run_id} 에 붙인다.</b>
     * 게다가 게이트가 쓰는 {@code asOf} 의 정본이 그 행에서 나온다({@code bin/seed.py} 가
     * {@code MAX(as_of)} 로 뽑는다) — 우연히 겹치는 게 아니라 <b>정상 절차가 반드시 겹친다.</b>
     *
     * <p>이어받으면 {@code countOf}·{@code checksumOf} 가 <b>배치 검출과 시드 정답의 합집합</b>을
     * 센다. 검증기가 한 건도 못 잡아도 {@code finding_count=800} 에 정답 checksum 이 나오고,
     * {@code finalizeRunStep} 이 시드가 남긴 기준값을 덮어쓴다 —
     * <b>게이트는 통과하는데 아무것도 증명하지 못한다.</b>
     *
     * <p>재사용이 막으려던 것("이 Step 이 COMPLETED 인데 잡 컨텍스트가 저장 전")은
     * {@code preventRestart()} 때문에 <b>도달할 수 없다.</b> 그래서 열린 실행만 이어받는다.
     */
    @Bean
    public Step startRunStep(
            VerificationRunRepository runs,
            ExpectedFindingRepository expectedFindings,
            ReplayHistoryRepository histories,
            VerificationRuleRepository rules,
            @Value("${batch.scheduling.enabled:true}") boolean schedulingEnabled
    ) {
        return new StepBuilder("startRunStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    StepExecution stepExecution = chunkContext.getStepContext().getStepExecution();
                    JobParameters parameters = stepExecution.getJobParameters();

                    LocalDateTime asOf = parameters.getLocalDateTime("asOf");
                    ScopeType scope = ScopeType.valueOf(parameters.getString("scope"));
                    DatasetType dataset = DatasetType.valueOf(parameters.getString("dataset"));
                    rejectDatasetMismatch(dataset, rules);
                    int attempt = parameters.getLong("attempt").intValue();

                    rejectUnsupportedScope(scope);
                    rejectRunningSchedulers(schedulingEnabled);

                    // 창이 없어도 마지막 이력 시각은 온다. ifPresent 안에 검사를 넣으면
                    // asOf 가 모든 이력보다 앞서는 경우 — 거부해야 하는 바로 그 경우 — 를 건너뛴다.
                    Optional<ReplayScanRange> scanRange = histories.scanRange(asOf);
                    scanRange.ifPresent(range -> rejectAsOfBeforeLatestHistory(asOf, range));
                    rejectIssuancesUpdatedAfterAsOf(asOf, rules.hasIssuancesUpdatedAfter(asOf));
                    rejectStocksUpdatedAfterAsOf(asOf, rules.hasStocksUpdatedAfter(asOf));

                    // runId 가 필요 없는 가드는 전부 INSERT 앞에 모인다.
                    Long seedRunId = validateSeedRunId(dataset, parameters, expectedFindings);

                    long runId = runs
                            .findByParams(asOf, dataset, scope, attempt)
                            .map(VerifyJobConfig::rejectExistingRun)
                            .orElseGet(() -> runs.save(VerificationRun.start(
                                    asOf, parameters.getLocalDateTime("fromTs"),
                                    scope, dataset, attempt,
                                    stepExecution.getJobExecution().getStartTime())).id());

                    ExecutionContext jobContext =
                            stepExecution.getJobExecution().getExecutionContext();
                    jobContext.putLong(RUN_ID_KEY, runId);
                    freezeSeedRunId(jobContext, seedRunId, expectedFindings);
                    jobContext.putString(POLICY_DIGEST_KEY, rules.policyDigest());
                    scanRange.filter(ReplayScanRange::hasWindow)
                            .ifPresent(range -> freeze(jobContext, range));

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
        // 창이 있을 때만 컨텍스트에 심으므로, 여기 값이 없으면 접을 것이 없는 실행이다.
        ReplayScanRange scanRange = minIssuanceId == null
                ? null
                : new ReplayScanRange(LocalDateTime.parse(latestCreatedAt),
                        minIssuanceId, maxIssuanceId, maxHistoryId);

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
     * <b>라벨과 실제 스키마가 같은지 본다.</b> {@code dataset} 은 {@code verification_runs} 에
     * 적히는 이름일 뿐이고, 어느 DB 를 읽을지는 접속 URL 이 정한다. 둘이 어긋나면
     * <b>CORRUPT 를 보면서 "CLEAN 에서 0건" 으로 기록</b>되고, 나중에 그 기록만 보고
     * 검증기를 뒤지게 된다 — 이 저장소가 반복해서 막아 온 "0건이 두 뜻을 갖는다" 와 같은 종류다.
     *
     * <p>실행 <b>전에</b> 죽어야 한다. 검출을 다 쌓은 뒤에 알면 그 run 을 통째로 버려야 한다.
     */
    private static void rejectDatasetMismatch(DatasetType dataset, VerificationRuleRepository rules) {
        boolean cleanShaped = rules.hasCleanOnlyConstraints();

        if (dataset == DatasetType.CLEAN && !cleanShaped) {
            throw new BusinessException(
                    VerificationErrorCode.INVALID_RUN_PARAMS,
                    "dataset=CLEAN 인데 uk_coupon_member 가 없습니다. CORRUPT 스키마를 보고 있습니다.");
        }
        if (dataset == DatasetType.CORRUPT && cleanShaped) {
            throw new BusinessException(
                    VerificationErrorCode.INVALID_RUN_PARAMS,
                    "dataset=CORRUPT 인데 uk_coupon_member 가 살아 있습니다. "
                            + "오염을 심을 수 없는 스키마라 검출 0건이 정상으로 보입니다.");
        }
    }

    /**
     * 증분 검증은 아직 구현되지 않았다. 받아 놓고 전수로 돌면 10분마다 534만 행을 다시 접고
     * {@code asof_state} 가 하루 4억 행 쌓이는데, 기록에는 INCREMENTAL 로 남아 티가 안 난다.
     */
    private static void rejectUnsupportedScope(ScopeType scope) {
        if (scope == ScopeType.INCREMENTAL) {
            throw new BusinessException(
                    VerificationErrorCode.INVALID_RUN_PARAMS,
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
            throw new BusinessException(
                    VerificationErrorCode.INVALID_AS_OF,
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
    private static void rejectIssuancesUpdatedAfterAsOf(LocalDateTime asOf, boolean updatedAfter) {
        if (updatedAfter) {
            throw new BusinessException(
                    VerificationErrorCode.DATASET_MUTATED_DURING_RUN,
                    "asOf 이후에 갱신된 발급건이 있습니다. 런타임과 스케줄러를 멈추고 다시 실행하십시오. "
                            + "asOf=" + asOf);
        }
    }

    /**
     * 재고 축 가드. 발급건 축과 대칭이다.
     *
     * <p>V1 이 접은 활성 건수와 <b>현재</b> {@code coupon_stocks.active_count} 를 비교하므로,
     * 배치가 도는 동안 발급이 한 건만 일어나도 그 회차가 어긋난 것으로 잡힌다.
     * 그냥 빼면 <b>0건이 두 뜻을 갖는다</b> — "제대로 훑고 없었다" 와 "훑을 대상이 안 남았다".
     */
    private static void rejectStocksUpdatedAfterAsOf(LocalDateTime asOf, boolean updatedAfter) {
        if (updatedAfter) {
            throw new BusinessException(
                    VerificationErrorCode.DATASET_MUTATED_DURING_RUN,
                    "asOf 이후에 갱신된 재고가 있습니다. 런타임과 스케줄러를 멈추고 다시 실행하십시오. "
                            + "asOf=" + asOf);
        }
    }

    /**
     * 스케줄러가 켜진 채로는 검증하지 않는다.
     *
     * <p>스케줄러가 이 프로세스 안에 함께 있다. 만료 스케줄러가 5분마다 {@code issuances} 를
     * 건드리므로, 켜둔 채로 돌리면 실행 도중 갱신이 거의 확실히 일어난다. 런북이 "먼저 끈다" 를
     * 요구하는데 지금까지 그것을 강제하는 수단이 없었다.
     */
    private static void rejectRunningSchedulers(boolean schedulingEnabled) {
        if (schedulingEnabled) {
            throw new BusinessException(
                    VerificationErrorCode.RUNTIME_NOT_QUIESCED,
                    "스케줄러가 켜진 상태에서는 검증할 수 없습니다. "
                            + "batch.scheduling.enabled=false 로 두고 다시 실행하십시오.");
        }
    }

    private static void freeze(ExecutionContext jobContext, ReplayScanRange range) {
        jobContext.putLong(SCAN_MIN_KEY, range.minIssuanceId());
        jobContext.putLong(SCAN_MAX_KEY, range.maxIssuanceId());
        jobContext.putLong(SCAN_MAX_HISTORY_KEY, range.maxHistoryId());
        jobContext.putString(SCAN_LATEST_KEY, range.latestCreatedAt().toString());
    }

    /** 얼림 확인이 자기 트랜잭션에서 읽어 둔 지문. 없으면 그 Step 이 안 돈 것이다. */
    private static String frozenFingerprint(JobExecution jobExecution) {
        Object value = jobExecution.getExecutionContext().get(FINGERPRINT_KEY);
        if (value == null) {
            throw new IllegalStateException(
                    "데이터셋 지문이 없습니다. assertFrozenStep 이 먼저 돌아야 합니다.");
        }
        return (String) value;
    }

    /**
     * <b>같은 파라미터의 실행이 이미 있으면 무조건 거부한다.</b> 이어받지 않는다.
     *
     * <p><b>닫힌 실행은 시드가 심은 기준 행이다.</b> {@code cy-seed/seedgen/stats.py} 가 적재할 때마다
     * {@code verification_runs} 를 심고 CORRUPT 는 <b>정답 800행을 그 {@code run_id} 에 붙인다.</b>
     * 게이트가 쓰는 {@code asOf} 도 그 행에서 나오므로 정상 절차가 반드시 겹친다.
     * 이어받으면 {@code countOf}·{@code checksumOf} 가 배치 검출과 정답의 합집합을 세어,
     * <b>검증기가 한 건도 못 잡아도 정답이 나온다.</b>
     *
     * <p><b>열린 실행도 이어받지 않는다.</b> 그 상태는 앞 실행이 중간에 죽어 남은 것인데,
     * {@code asof_state} 에 그 실행의 <b>부분 결과가 남아 있다.</b> 검출이 0건이라 통과시키면
     * 새 리플레이가 UPSERT 로 덮지 못한 낡은 행이 그대로 V1·V3·V5 에 섞인다.
     * 게다가 {@code preventRestart()} 때문에 그 상황은 배치 메타DB 와 업무DB 가 어긋났을 때만
     * 열린다 — 이어받아 될 상태가 아니다.
     *
     * <p>시드가 CLEAN {@code attempt} 1·2, CORRUPT 1 을 점유하므로
     * <b>배치는 CLEAN 3 · CORRUPT 2 부터</b> 시작한다.
     */
    private static long rejectExistingRun(VerificationRun existing) {
        throw new BusinessException(
                VerificationErrorCode.INVALID_RUN_PARAMS,
                "같은 파라미터의 실행이 이미 있습니다. 시드가 심은 기준 실행이거나 앞 실행이 남긴 "
                        + "부분 결과일 수 있어 이어받지 않습니다. attempt 를 올리십시오 "
                        + "(시드가 CLEAN 1·2, CORRUPT 1 을 점유합니다). runId=" + existing.id()
                        + " attempt=" + existing.attempt());
    }

    /**
     * <b>오염셋은 집합이 같아야 합격이다.</b> 개수만 보면 오탐 400 + 누락 400 도 800 이라,
     * 정확히 검출한 것과 구분되지 않는다.
     *
     * <p><b>정답 묶음이 없으면 판정하지 않고 죽인다.</b> 그대로 두면 검출 전부가 오탐으로
     * 잡혀 "오탐 800" 이라는 엉뚱한 결론이 나오고, 원인(주입을 안 돌렸다)이 안 보인다.
     *
     * <p><b>불일치는 예외가 아니라 판정이다.</b> 검증은 정상적으로 끝났고 답이 "다르다" 인 것이므로
     * {@code FAIL} 을 돌려주고 checksum·지문도 함께 기록된다. 실행 실패({@code FAILED})와 구분한다.
     * 무엇이 어긋났는지는 Step 종료 메시지에 싣는다 — batch 모듈에는 로깅 채널이 없다.
     *
     * <p><b>합격에도 메시지를 남긴다.</b> 판정 근거인 {@code seedRunId} 는 잡 실행 컨텍스트에만
     * 있고 {@code verification_runs} 에는 컬럼이 없다. 그대로 두면 PASS 행 하나가
     * <i>"어느 묶음과 대조해서 통과했는지"</i> 를 못 댄다 — 주입을 두 번 돌려 묶음이 둘인 DB 에서
     * 정확히 그 질문이 온다.
     *
     * <p><b>Step 종료 코드는 프로세스 종료코드가 아니다.</b> 한때 이 자리에
     * <i>"COMPLETED 면 Boot 가 종료코드 0 으로 매핑하니 CI 가 {@code $?} 만 보면 통과한다"</i>
     * 고 적어 두었는데 셋 다 틀렸다 — ⑴ {@code JobExecutionExitCodeGenerator.getExitCode()} 가
     * 읽는 것은 {@code ExitStatus} 가 아니라 {@code BatchStatus} 이고 {@code COMPLETED} 는
     * {@code ordinal() == 0} 이다, ⑵ {@code BatchApplication.main} 이
     * {@code System.exit(SpringApplication.exit(..))} 를 안 불러 생성기가 종료코드로 이어지지도
     * 않는다, ⑶ {@code web-application-type: servlet} 이라 프로세스가 끝나지 않는다.
     * 게다가 {@code SimpleJob} 은 잡 종료 코드를 <b>마지막 Step 값으로 대입</b>하므로
     * ({@code javap -c SimpleJob}) 통계 Step 이 뒤에 붙는 순간 이 표식은 잡 수준에서 사라진다.
     * <b>게이트는 {@code verification_runs.verdict} 를 읽어야 한다.</b> 여기 {@code ExitStatus}
     * 는 배치 메타DB({@code BATCH_STEP_EXECUTION})에 남기는 표식일 뿐이다.
     */
    private static VerdictType judgeAgainstManifest(
            long runId, long seedRunId, int detected,
            ExpectedFindingRepository expected, StepContribution contribution) {
        // startRunStep 이 이미 확인했다. 실행 중에 매니페스트가 지워진 경우만 여기 온다 —
        // 그건 판정이 아니라 사고이므로 부재와 같은 코드로 죽인다.
        if (!expected.exists(seedRunId)) {
            throw new BusinessException(
                    VerificationErrorCode.MANIFEST_ABSENT,
                    "실행 중에 정답 매니페스트가 사라졌습니다. seedRunId=" + seedRunId);
        }

        // 판정 입력이 실행 중에 바뀌면 같은 데이터·같은 asOf 인데 판정만 달라진다.
        // 데이터 네 축은 assertFrozenStep 이 얼리는데 매니페스트는 그 Step 뒤에 읽히므로
        // 여기서 본다 — 아래 두 질의와 같은 트랜잭션이라 스냅샷이 일치한다.
        String digest = expected.digestOf(seedRunId);
        if (!frozenManifestDigest(contribution).equals(digest)) {
            throw new BusinessException(
                    VerificationErrorCode.DATASET_MUTATED_DURING_RUN,
                    "정답 매니페스트가 실행 중에 바뀌었습니다. 주입을 다시 돌렸다면 검증도 "
                            + "다시 시작해야 합니다. seedRunId=" + seedRunId
                            + " 시작=" + frozenManifestDigest(contribution) + " 지금=" + digest);
        }

        List<FindingKey> missing = expected.missing(runId, seedRunId);
        List<FindingKey> unexpected = expected.unexpected(runId, seedRunId);

        if (missing.isEmpty() && unexpected.isEmpty()) {
            contribution.setExitStatus(ExitStatus.COMPLETED.addExitDescription(
                    "seedRunId=" + seedRunId + " · 정답 " + expected.countOf(seedRunId)
                            + "건 / 검출 " + detected + "건 · 집합 일치"));

            return VerdictType.PASS;
        }

        contribution.setExitStatus(new ExitStatus(ExitStatus.FAILED.getExitCode(),
                "정답 " + expected.countOf(seedRunId) + "건 / 검출 " + detected
                        + "건 · 누락 " + missing.size() + "건 " + sample(missing)
                        + " · 오탐 " + unexpected.size() + "건 " + sample(unexpected)));

        return VerdictType.FAIL;
    }

    /**
     * 표본만 싣는다. 수천 건이 메시지를 덮으면 정작 개수를 못 읽는다.
     *
     * <p><b>양쪽을 함께 싣는다.</b> 한쪽만 보이면 상쇄된 오차를 못 읽는다 —
     * 누락과 오탐이 같은 수면 검출 개수는 정답과 똑같다.
     */
    private static List<String> sample(List<FindingKey> keys) {
        return keys.stream().limit(SAMPLE_SIZE).map(FindingKey::toString).toList();
    }

    /**
     * <b>실행 전에 죽어야 한다.</b> 매니페스트가 없거나 {@code seedRunId} 가 틀린 것은
     * 1초 만에 알 수 있는데, 마지막 Step 에서 알면 리플레이 300만 건과 규칙 여섯을
     * 다 돌린 뒤라 그 실행을 통째로 버려야 한다 — 재사용도 못 하니 {@code attempt} 를
     * 올려 처음부터다.
     *
     * <p><b>CORRUPT 는 {@code seedRunId} 를 반드시 받는다.</b> 기본값을 두면 정답 묶음이
     * 둘 이상인 DB 에서 <b>조용히 낡은 묶음과 대조</b>한다 — 주입을 두 번 돌리면 실제로 그렇게 된다.
     * 그 사고는 "누락 800 · 오탐 800" 으로 나타나 규칙을 의심하게 만든다.
     *
     * <p><b>참조 구현은 기본값 1 을 둔다</b>({@code cy-seed/bin/seed.py} 의 {@code --seed-run-id}).
     * 여기서는 <b>일부러 다르게 간다</b> — 시드는 방금 주입한 묶음을 같은 프로세스 안에서
     * 대조하지만, 배치는 <b>남의 DB 를 나중에 읽으므로</b> 기본값이 곧 "낡은 묶음과 조용히 대조" 다.
     */
    private static Long validateSeedRunId(
            DatasetType dataset,
            JobParameters parameters,
            ExpectedFindingRepository expected) {
        if (dataset == DatasetType.CLEAN) {
            return null;
        }

        JobParameter<?> parameter = parameters.getParameter("seedRunId");
        if (parameter != null && parameter.identifying()) {
            throw new BusinessException(
                    VerificationErrorCode.INVALID_RUN_PARAMS,
                    "seedRunId 는 비식별이어야 합니다. 식별로 넣으면 Spring Batch 가 새 JobInstance 로 "
                            + "받아 잡을 시작하는데, uk_run_params(as_of, dataset, scope, attempt) 에는 "
                            + "그 축이 없어 '같은 파라미터의 실행이 이미 있습니다' 로 죽습니다 — "
                            + "파라미터를 바꿔 던졌는데 그런 메시지가 나옵니다. "
                            + "seedRunId=" + parameter.value() + ",java.lang.Long,false 로 던지십시오.");
        }

        Long seedRunId = parameters.getLong("seedRunId");
        if (seedRunId == null) {
            throw new BusinessException(
                    VerificationErrorCode.INVALID_RUN_PARAMS,
                    "오염셋 검증에는 seedRunId 가 필요합니다. 기본값을 두면 정답 묶음이 여럿일 때 "
                            + "낡은 것과 조용히 대조합니다. 예: seedRunId=1,java.lang.Long,false "
                            + "(끝의 false 가 비식별이다. 빼면 기본이 식별이라 JobInstance 축이 "
                            + "uk_run_params 와 어긋나 재실행이 엉뚱한 이유로 죽는다)");
        }
        if (!expected.exists(seedRunId)) {
            throw new BusinessException(
                    VerificationErrorCode.MANIFEST_ABSENT,
                    "정답 매니페스트가 없습니다. 오염 주입을 돌리지 않았거나 seedRunId 가 "
                            + "틀렸습니다. seedRunId=" + seedRunId);
        }

        return seedRunId;
    }

    /**
     * <b>얼리는 것과 검증하는 것을 나눈다.</b> 검증은 {@code runId} 가 없어도 되므로 실행 행을
     * INSERT 하기 <b>전에</b> 끝낸다 — 나머지 여섯 가드가 전부 그 앞에 있고, 이것만 뒤에 있으면
     * 안전성이 코드 순서가 아니라 tasklet 롤백이라는 암묵 계약에 걸린다. 그 계약이 깨지는 날
     * ({@code startRunStep} 을 서비스로 뽑거나 전파 속성을 바꾸는 날) {@code seedRunId} 오타
     * 하나가 {@code verification_runs} 에 열린 행을 남기고, {@code uk_run_params} 때문에
     * <b>오타를 고쳐도 같은 파라미터로 다시 못 돌린다.</b>
     */
    private static void freezeSeedRunId(
            ExecutionContext jobContext, Long seedRunId, ExpectedFindingRepository expected) {
        if (seedRunId == null) {
            return;
        }

        jobContext.putLong(SEED_RUN_ID_KEY, seedRunId);
        jobContext.putString(MANIFEST_DIGEST_KEY, expected.digestOf(seedRunId));
    }

    /** {@code startRunStep} 이 얼려 둔 정답 묶음 지문. */
    private static String frozenManifestDigest(StepContribution contribution) {
        Object value = contribution.getStepExecution().getJobExecution()
                .getExecutionContext().get(MANIFEST_DIGEST_KEY);
        if (value == null) {
            throw new IllegalStateException(
                    "매니페스트 지문이 없습니다. startRunStep 이 먼저 돌아야 합니다.");
        }
        return (String) value;
    }

    /** {@code startRunStep} 이 얼려 둔 값. 거기서 존재 확인까지 끝냈다. */
    private static long frozenSeedRunId(JobExecution jobExecution) {
        Object value = jobExecution.getExecutionContext().get(SEED_RUN_ID_KEY);
        if (value == null) {
            throw new IllegalStateException(
                    "seedRunId 가 없습니다. startRunStep 이 먼저 돌아야 합니다.");
        }
        return ((Number) value).longValue();
    }

    /**
     * <b>CLEAN 전용이다.</b> 정상셋은 "검출 0건 = 통과" 가 규칙만으로 결정된다.
     * 오염셋은 {@link #judgeAgainstManifest} 가 정답과의 집합 일치로 판정한다.
     */
    static VerdictType verdictOfClean(int detected) {
        return detected == 0 ? VerdictType.PASS : VerdictType.FAIL;
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

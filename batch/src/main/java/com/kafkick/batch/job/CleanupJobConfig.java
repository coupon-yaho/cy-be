// 검증이 남긴 파생 행과 배치 메타를 걷는 잡의 배선입니다.
package com.kafkick.batch.job;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;

import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.verification.CleanupRepository;
import com.kafkick.core.verification.StatsRepository;

/**
 * <b>{@code @Scheduled} 가 아니라 Spring Batch 잡이다 — 규칙의 입력이 바뀌었다.</b>
 *
 * <p>{@code docs/11} 은 정리를 계층 3({@code @Scheduled})으로 분류했고, 그 규칙은
 * <i>"청크 재시작 · 실행 이력이 판정 근거 · 파라미터 재실행 증명 중 하나도 없으면
 * {@code @Scheduled}"</i> 다. 정리는 셋 다 해당 없다.
 *
 * <p><b>그런데 CY-392 가 감시를 전부 배치 메타 위에 세웠다.</b>
 * {@code BatchRunMetrics} 는 {@code List<Job>} 빈에서 이름을 모아 잡마다 마지막 성공 시각
 * 게이지를 <b>자동으로</b> 만들고, {@code BatchJobFailed}·{@code BatchStuckExecution} 은
 * 셀렉터에 잡 이름이 없어 새 잡을 그날 바로 덮는다. {@code @Scheduled} 로 만들면
 * {@code BATCH_JOB_EXECUTION} 행이 안 남아 <b>그 감시가 전부 비껴간다.</b>
 *
 * <p>정리는 300만 행을 지우는 일이라 <b>조용히 안 도는 것</b>이 가장 나쁜 결말이다.
 * 그래서 잡으로 만든다 — 규칙을 어긴 것이 아니라, 규칙이 세워질 때 없던 축이 생긴 것이다.
 *
 * <p><b>무엇을 지우나.</b> 검증이 남긴 파생 행 셋이다 — {@code asof_state} · 통계 셋 ·
 * {@code verification_findings}. {@code verification_runs} 행 자체는 남긴다 — 그것이 판정
 * 이력이고 관제와 지표가 그 위에 선다. {@code FAIL} 의 검출 행도 남긴다({@code deleteFindings}
 * 의 근거). <b>{@code BATCH_*} 메타는 아직 안 지운다</b> — {@code docs/13} §7 과 같은 축이라
 * 뒤로 미뤘다.
 *
 * <p><b>멱등성 레코드와 토큰은 안 지운다.</b> {@code idempotency_records} 는 읽고 쓰는 코드가
 * 저장소에 <b>하나도 없고</b>, 토큰은 애초에 이 저장소의 테이블이 아니다(Redis, 영역 ③).
 * {@code docs/11} 의 목록이 그 둘을 적은 것은 <b>계획</b>이지 현재 상태가 아니다.
 */
@Configuration(proxyBeanMethods = false)
public class CleanupJobConfig {

    /**
     * <b>{@code BatchRunMetrics} 가 이 이름으로 게이지를 만든다.</b> 잡 이름은 그 축의
     * 라벨 값이 되므로 리터럴을 두 곳에 적지 않는다.
     */
    public static final String JOB_NAME = "cleanupJob";

    /**
     * <b>어디까지 끝냈나.</b> 대상은 id 오름차순이라 <i>"이 id 까지는 셋 다 걷었다"</i> 하나로
     * 진도가 표현된다.
     *
     * <p><b>실행 컨텍스트에 두는 이유는 재시작이 아니라 격리다.</b> 태스클릿 람다는 싱글턴
     * 빈 안에 있어서 필드에 두면 <b>실행끼리 값이 샌다.</b> 재시작으로 이어받지는 못한다 —
     * {@code CleanupScheduler} 가 매 발화에 {@code firedAt} 을 실어 <b>매번 새 JobInstance</b>
     * 를 만들고, 실패한 실행을 다시 시작하는 경로도 없다. 실패하면 다음 발화가 처음부터
     * 다시 훑는다. 지우는 일은 멱등이라 손해는 재스캔뿐이다.
     */
    private static final String DONE_UP_TO_KEY = "cleanup.doneUpToRunId";

    /** 로그용 누계. 청크마다 트랜잭션이 갈리므로 지역 변수로는 살아남지 못한다. */
    private static final String ASOF_STATE_ROWS_KEY = "cleanup.asOfStateRows";

    /** 지금 실행에서 걷은 행 수. "재방문" 과 "실제로 걷었다" 를 가르는 값이다. */
    private static final String CURRENT_RUN_ROWS_KEY = "cleanup.currentRunRows";

    private static final String FINDING_ROWS_KEY = "cleanup.findingRows";

    private static final String RUNS_PURGED_KEY = "cleanup.runsPurged";

    /**
     * <b>걷을 것이 남았는데 검증에 자리를 내주고 멈춘 상태.</b> 잡 상태는 {@code COMPLETED}
     * 로 둔다 — 실패가 아니고, 실패로 두면 {@code BatchJobFailed} 가 검증 때문에 운다.
     * 대신 종료 코드로 사실을 남겨 {@code BatchRunMetricsRefresher} 가 성공 집계에서 뺀다.
     *
     * <p><b>"한 행도 안 걷었다" 가 아니다.</b> 검사는 청크마다 도므로 <b>커밋된 진도까지는
     * 남는다</b> — 몇 행에서 멈췄는지는 종료 <i>설명</i>({@code purgedRows})이 진다.
     * {@code ExitStatus.and()} 의 severity 순서상 미지 코드가 {@code COMPLETED} 를 이기므로,
     * 마지막 청크에서 한 번만 세워도 Step·Job 종료 코드가 이것이 된다.
     *
     * <p><b>걷을 것이 없던 밤은 여기 해당하지 않는다.</b> 대상 조회가 이 검사보다 앞에
     * 있어서, 할 일이 없으면 검증이 떠 있어도 그냥 {@code COMPLETED} 로 닫힌다 —
     * 순서가 반대였을 때는 정상 상태에서 {@code CleanupNotSucceeding} 이 울었다.
     *
     * <p><b>물러나는 횟수에는 상한이 없다.</b> 만료의 {@code max-expire-skips} 같은 장치를
     * 안 뒀다 — 대신 반복되는 yield 는 {@code CleanupNotSucceeding} 이 25시간째에 잡는다.
     * 마지막 성공이 안 갱신되기 때문이다.
     */
    public static final String YIELDED_EXIT_CODE = "YIELDED";

    private static final Logger log = LoggerFactory.getLogger(CleanupJobConfig.class);

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final TransactionAttribute timeout;
    private final int keepRuns;
    private final int chunkSize;
    private final long abandonedAfterHours;

    /**
     * <b>값 검사를 생성자에 모은다.</b> Step 빈 메서드 파라미터로 두면 컨테이너를 띄워야만
     * 재지는데, 이 검사들은 DB 와 아무 상관이 없다 — {@code ExpireJobConfig} 가 같은 이유로
     * 같은 모양이고 {@code ExpireJobSettingsTest} 가 컨테이너 없이 그것을 잰다.
     */
    public CleanupJobConfig(JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            @Value("${batch.cleanup.step-timeout-ms:120000}") long stepTimeoutMillis,
            @Value("${batch.verify.asof-state-keep-runs:5}") int keepRuns,
            @Value("${batch.cleanup.chunk-size:10000}") int chunkSize,
            @Value("${batch.cleanup.abandoned-after-hours:24}") long abandonedAfterHours) {
        // 스프링의 트랜잭션 타임아웃은 초 단위라 999 는 0 으로 내려앉는데, 0 은 "무제한" 이
        // 아니라 **데드라인이 이미 지났음**이다 — 첫 문장에서 TransactionTimedOutException 이
        // 난다. 기동은 성공하고 04:30 만 매일 조용히 실패하는 모양이 되므로 여기서 거절한다.
        // VerifyJobConfig·ExpireJobConfig 가 각자 같은 가드를 갖고 있다.
        if (stepTimeoutMillis < 1_000 || stepTimeoutMillis % 1_000 != 0) {
            throw new IllegalArgumentException(
                    "batch.cleanup.step-timeout-ms 는 1000 이상이면서 1000 의 배수여야 합니다. "
                            + "초 단위로 내림하기 때문에 999 이하는 0 초가 되고, 그러면 첫 "
                            + "문장에서 트랜잭션이 만료됩니다. 받은 값=" + stepTimeoutMillis);
        }
        // chunk-size 0 은 DELETE … LIMIT 0 이라 **한 행도 안 지운 채 성공으로 닫힌다.**
        // 잡이 COMPLETED 라 CleanupNotSucceeding 도 안 울고, 가장 무거운 테이블만 안 걷히는
        // 상태가 감시망을 통째로 통과한다. 음수는 MySQL 문법 오류로 매일 실패한다.
        if (chunkSize < 1) {
            throw new IllegalArgumentException(
                    "batch.cleanup.chunk-size 는 1 이상이어야 합니다. 0 이면 asof_state 를 "
                            + "한 행도 안 지운 채 잡이 성공으로 닫혀 감시망을 통과합니다. "
                            + "받은 값=" + chunkSize);
        }
        // 0 이면 방금 시작한 검증까지 "버려진 것" 으로 보고 그 입력을 걷는다.
        if (abandonedAfterHours < 1) {
            throw new IllegalArgumentException(
                    "batch.cleanup.abandoned-after-hours 는 1 이상이어야 합니다. 0 이면 방금 "
                            + "시작한 검증의 asof_state 를 걷어 그 판정이 자기 입력을 잃습니다. "
                            + "받은 값=" + abandonedAfterHours);
        }
        // 0 이면 방금 끝난 판정의 파생 행까지 그날 밤에 사라진다.
        if (keepRuns < 1) {
            throw new IllegalArgumentException(
                    "batch.verify.asof-state-keep-runs 는 1 이상이어야 합니다. 0 이면 직전 "
                            + "판정의 파생 행이 그날 밤에 사라져 되짚을 근거가 없습니다. "
                            + "받은 값=" + keepRuns);
        }
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.keepRuns = keepRuns;
        this.chunkSize = chunkSize;
        this.abandonedAfterHours = abandonedAfterHours;
        DefaultTransactionAttribute attribute = new DefaultTransactionAttribute();
        attribute.setTimeout(Math.toIntExact(stepTimeoutMillis / 1_000));
        attribute.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.timeout = attribute;
    }

    @Bean
    public Job cleanupJob(Step purgeVerificationRunsStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(purgeVerificationRunsStep)
                .build();
    }

    /**
     * <b>파라미터가 없다.</b> 정리는 <i>"지금 남아 있는 것"</i> 을 보는 일이라 기준 시각을
     * 실을 이유가 없고, 실으면 같은 값으로 두 번 못 도는 제약만 생긴다 — 만료가 {@code asOf}
     * 를 식별 파라미터로 쓰는 것과 반대 방향이다.
     *
     * <p>대신 {@code JobParametersIncrementer} 도 안 쓴다. 스케줄러가 매 실행에 시각을
     * 실어 인스턴스를 가른다({@code CleanupScheduler}).
     *
     * <p><b>태스클릿 한 번이 청크 하나다.</b> {@code RepeatStatus.CONTINUABLE} 로 돌아오면
     * Spring Batch 가 <b>새 트랜잭션</b>에서 다시 부르므로 앞 청크는 커밋된 채 남는다.
     * 한 번에 다 돌고 {@code FINISHED} 를 돌려주면 <b>청킹이 아무것도 안 나눈다</b> —
     * {@code LIMIT} 을 붙여 삭제해도 커밋이 마지막에 한 번뿐이라 언두 로그와 잠금은 전량을
     * 통째로 들고 있고, 데드라인에 걸리면 <b>여태 지운 것이 전부 롤백</b>된다. 그러면 진도가
     * 0 이라 다음 날도 같은 양을 처음부터 시도해 또 실패한다 — 영원히 성공 못 하는 잡이 된다.
     * {@code ExpireJobConfig} 가 같은 이유로 같은 모양을 쓴다.
     *
     * <p><b>진도는 "어디까지 끝냈나" 하나로 충분하다.</b> 대상 목록을 컨텍스트에 실어 두지
     * 않고 매 호출 다시 고른다 — {@code verification_runs} 는 수백 행이라 값이 싸고,
     * 목록을 얼려 두면 그 사이 판정이 난 실행을 <b>옛 판단으로</b> 지우게 된다.
     *
     * <p><b>대상 집합은 "이미 걷었나" 를 안 본다 — 매 밤 지난 이력을 다시 들른다.</b>
     * 이미 빈 실행 하나마다 태스클릿이 한 번 더 돌고 그 한 번이 독립 트랜잭션이다.
     * 3주 시연 규모(수백 행)에서는 수백 번의 빈 왕복이라 초 단위이고,
     * {@code CleanupRunningTooLong}(900초) 근처에도 못 간다 — 그래서 지금은 술어를 안 더한다.
     * 이력이 만 단위로 가면 {@code verification_runs.purged_at} 을 세워 잘라야 한다.
     * 대신 <b>로그가 재방문을 "걷었다" 로 세지 않게</b> 했다.
     */
    @Bean
    public Step purgeVerificationRunsStep(CleanupRepository cleanup, StatsRepository stats,
            RunningJobProbe runningJobs, TimeProvider timeProvider) {
        return new StepBuilder("purgeVerificationRunsStep", jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    ExecutionContext context =
                            chunkContext.getStepContext().getStepExecution().getExecutionContext();

                    // **"걷을 것이 있나" 를 먼저 묻는다.** 순서가 반대면 할 일이 없던 밤에도
                    // 검증이 떠 있다는 이유만으로 YIELDED 가 되고, 그 실행이 마지막 성공에서
                    // 빠져 **정상 상태에서 CleanupNotSucceeding 이 운다.** 이 두 질의는
                    // SELECT 뿐이라 검증과 부딪히지 않는다.
                    LocalDateTime abandonedBefore =
                            timeProvider.now().minusHours(abandonedAfterHours);
                    long doneUpTo = context.getLong(DONE_UP_TO_KEY, 0);
                    Optional<Long> next =
                            nextTarget(cleanup, keepRuns, abandonedBefore, doneUpTo);
                    if (next.isEmpty()) {
                        log.info("검증 파생 행을 걷었습니다. 실행={}개 asof_state={}행 findings={}행",
                                context.getLong(RUNS_PURGED_KEY, 0),
                                context.getLong(ASOF_STATE_ROWS_KEY, 0),
                                context.getLong(FINDING_ROWS_KEY, 0));
                        return RepeatStatus.FINISHED;
                    }

                    // **시각 창 하나에 파괴적 삭제를 맡기지 않는다.** abandoned-after-hours 의
                    // 근거는 "300만 전수의 소요를 아직 안 쟀다"(docs/13 §6 의 D) 뿐이라,
                    // 실제 소요가 그 값을 넘는 날 **도는 검증의 입력**이 걷힌다. 그때
                    // V1·V3·V5 는 빈 상태를 읽고 예외 없이 **조용히 틀린 답**을 낸다.
                    // 배치 메타에 같은 질문을 하는 수단이 이미 있으므로 그쪽을 먼저 본다.
                    //
                    // 매 청크마다 본다. 도중에 검증이 뜨면 거기서 멈추는 편이 맞고, 커밋된
                    // 데까지는 남아 다음 날이 이어받는다 — 지우는 일은 멱등이다.
                    List<Long> verifying = runningJobs.blockingExecutions(VerifyJobConfig.JOB_NAME);
                    if (!verifying.isEmpty()) {
                        long purged = context.getLong(ASOF_STATE_ROWS_KEY, 0)
                                + context.getLong(FINDING_ROWS_KEY, 0);
                        log.warn("검증이 도는 중이라 정리를 여기서 멈춥니다. 디스크는 하루 더 "
                                        + "기다릴 수 있지만 도는 검증의 입력은 되돌릴 수 없습니다. "
                                        + "verifyExecutionIds={} 이번에 걷은 행={}",
                                verifying, purged);
                        // **로그는 감시 수단이 아니다.** 그냥 FINISHED 로 물러나면 잡이
                        // COMPLETED 로 닫혀 cy_batch_last_success_seconds 가 갱신되고,
                        // 걷을 것이 남았는데 CleanupNotSucceeding 이 영원히 조용하다 —
                        // chunk-size=0 을 기동 때 거절한 근거와 관측상 똑같은 상태다.
                        // 그래서 배치 메타에 사실을 남긴다: 상태는 COMPLETED(실패가 아니다),
                        // 종료 코드는 YIELDED. BatchRunMetricsRefresher 가 그 코드를 성공에서
                        // 뺀다. statsAggregateStep 이 같은 이유로 ExitStatus("SKIPPED") 를 쓴다.
                        //
                        // **얼마나 걷고 멈췄는지를 설명에 싣는다.** 코드 하나로는 "한 행도 못
                        // 걷었다" 와 "200만 걷고 멈췄다" 가 같은 값이라, 그 구분을 배치 메타가
                        // 지게 한다 — 로그에만 두면 되짚을 때 남아 있지 않다.
                        contribution.setExitStatus(new ExitStatus(YIELDED_EXIT_CODE,
                                "verifyExecutionIds=" + verifying + " purgedRows=" + purged));
                        return RepeatStatus.FINISHED;
                    }

                    long runId = next.get();
                    int deleted = cleanup.deleteAsOfStateChunk(runId, chunkSize);
                    if (deleted > 0) {
                        add(context, ASOF_STATE_ROWS_KEY, deleted);
                        add(context, CURRENT_RUN_ROWS_KEY, deleted);
                        contribution.incrementWriteCount(deleted);
                        return RepeatStatus.CONTINUABLE;
                    }

                    // 이 실행의 asof_state 가 다 걷혔다. 나머지 둘은 건수가 작아 한 번에 지우고,
                    // 그때 비로소 진도를 옮긴다 — 셋이 같은 트랜잭션에서 끝나야 "걷은 실행" 의
                    // 뜻이 셋 모두를 덮는다.
                    int findings = cleanup.deleteFindings(runId);
                    // 통계 셋은 이미 있는 계약을 쓴다 — 세 테이블의 FK 순서를 아는 것이
                    // 그쪽이고, 여기서 다시 적으면 한쪽만 고치는 실수가 열린다.
                    stats.clear(runId);

                    add(context, FINDING_ROWS_KEY, findings);
                    // **재방문은 "걷었다" 로 세지 않는다.** 대상 집합은 파생 행이 남아 있는지를
                    // 안 보므로 이미 빈 실행도 매 밤 한 번씩 다시 들른다(아래 javadoc).
                    // 그때까지 세면 아무것도 안 지운 밤에 "실행 40개를 걷었습니다" 가 찍히는데,
                    // 사고를 되짚을 때 유일한 단서라 사실이어야 한다.
                    if (context.getLong(CURRENT_RUN_ROWS_KEY, 0) > 0 || findings > 0) {
                        add(context, RUNS_PURGED_KEY, 1);
                    }
                    context.putLong(CURRENT_RUN_ROWS_KEY, 0);
                    context.putLong(DONE_UP_TO_KEY, runId);
                    contribution.incrementWriteCount(findings);
                    return RepeatStatus.CONTINUABLE;
                }, transactionManager)
                .transactionAttribute(timeout)
                .build();
    }

    /**
     * 아직 안 끝낸 것 중 가장 오래된 실행. <b>id 오름차순</b>이라 진도 하나로 표현된다.
     *
     * <p>두 집합을 합친다. 겹칠 수 있고(오래된 버려진 실행), 합집합이라 두 번 지우지 않는다.
     */
    private static Optional<Long> nextTarget(CleanupRepository cleanup, int keepRuns,
            LocalDateTime abandonedBefore, long doneUpTo) {
        Set<Long> targets =
                new LinkedHashSet<>(cleanup.purgeableRunIds(keepRuns, abandonedBefore));
        targets.addAll(cleanup.abandonedRunIds(abandonedBefore));
        return targets.stream().filter(id -> id > doneUpTo).min(Long::compareTo);
    }

    private static void add(ExecutionContext context, String key, long delta) {
        context.putLong(key, context.getLong(key, 0) + delta);
    }
}

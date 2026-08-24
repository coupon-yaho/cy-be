// 어떤 잡이 지금 실제로 돌고 있는지를 배치 메타에 물어봅니다.
package com.kafkick.batch.config;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * <b>{@code batch.scheduling.enabled} 를 보던 자리를 대신한다.</b>
 *
 * <p>그 플래그는 <i>"스케줄러 빈이 만들어졌는가"</i>였다. 운영에서는 늘 켜져 있으므로 검증은
 * <b>영영 못 돈다</b> — 그래서 검증이 온디맨드로 밀렸고, 그 결과로 감시 공백이 생겼다.
 * 공백의 절반은 우리가 만든 것이라, 크론으로 옮기려면 이 자리부터 풀어야 한다.
 *
 * <p><b>대신 묻는 것은 "지금 그 잡이 도는 중인가" 다.</b> 근거는 배치 메타이고,
 * {@link JobRepository#findRunningJobExecutions(String)} 이
 * {@code STATUS IN ('STARTING','STARTED','STOPPING')} 인 행을 준다(6.0.4 바이트코드로 확인).
 *
 * <p><b>양방향으로 쓴다.</b> 검증은 만료가 도는 중이면 거절하고, 만료 스케줄러는 검증이
 * 도는 중이면 그 슬롯을 건너뛴다. 잡 이름을 인자로 받는 것이 그래서다.
 *
 * <p><b>이 검사는 정확성의 근거가 아니다.</b> 그것은 {@code assertFrozenStep} 이 진다 —
 * 규칙이 다 돈 뒤에 발급건·재고·회차 정책·이력 네 축을 다시 보고, 하나라도 움직였으면
 * {@code DATASET_MUTATED_DURING_RUN} 으로 판정을 버린다. 이 검사가 하는 일은
 * <b>헛돌지 않게 하는 것</b>이다.
 */
@Component
public class RunningJobProbe {

    private static final Logger log = LoggerFactory.getLogger(RunningJobProbe.class);

    private final JobRepository jobRepository;
    private final Duration stuckAfter;

    /**
     * <b>임계가 가장 긴 Step 데드라인보다 커야 한다 — 기동 때 못 박는다.</b>
     *
     * <p>하트비트는 <b>Step 경계와 청크 커밋에서만</b> 뛴다. 그런데 {@code verifyJob} 은
     * Step 열하나 중 {@code replayStep} 하나만 청크이고 나머지 열은
     * {@code RepeatStatus.FINISHED} 단발 태스클릿이다 — <b>그 Step 이 도는 내내 값이 안
     * 움직인다.</b> 그 침묵의 상한이 곧 {@code batch.verify.step-timeout-ms} 다.
     *
     * <p>두 값이 같으면 <b>정상적으로 오래 도는 검증이 시체로 판정되고</b>, 그러면 만료가
     * 검증 한복판을 지나가 판정이 버려진다. 그리고 만료가 찍은 {@code updated_at} 은 지워지지
     * 않으므로 <b>그 {@code asOf} 를 영구히 못 쓴다.</b> 한때 아래쪽(살아 있음을 뜻하는
     * 300초)을 임계로 써서 같은 사고를 냈는데, 이번에는 위쪽에서 같은 모양이 나왔다.
     *
     * <p>{@code .example} 이 <i>"chunk-size 를 키우면 step-timeout 도 같이 키워야 한다"</i>
     * 고 적어 두었으므로 그 조정은 <b>예정된 일</b>이다. 그때 이 관계가 깨지는 것을
     * 사람의 기억에 맡기지 않는다.
     */
    public RunningJobProbe(JobRepository jobRepository,
            @Value("${batch.stuck-job-after-ms:1800000}") long stuckAfterMs,
            @Value("${batch.verify.step-timeout-ms:600000}") long verifyStepTimeoutMs,
            @Value("${batch.expire.step-timeout-ms:120000}") long expireStepTimeoutMs,
            @Value("${batch.cleanup.step-timeout-ms:120000}") long cleanupStepTimeoutMs) {
        // 침묵할 수 있는 Step 을 전부 세지 않으면 이 가드가 지키는 관계가 조용히 깨진다.
        // 새 잡이 생길 때마다 여기 인자가 하나 는다 — 그것이 이 가드의 값이다.
        String longestName = "verify";
        long longestSilentStep = verifyStepTimeoutMs;
        if (expireStepTimeoutMs > longestSilentStep) {
            longestName = "expire";
            longestSilentStep = expireStepTimeoutMs;
        }
        if (cleanupStepTimeoutMs > longestSilentStep) {
            longestName = "cleanup";
            longestSilentStep = cleanupStepTimeoutMs;
        }
        if (stuckAfterMs <= longestSilentStep) {
            throw new IllegalArgumentException(
                    "batch.stuck-job-after-ms 는 가장 긴 Step 데드라인보다 커야 합니다. "
                            + "verifyJob 의 Step 열 개와 cleanupJob 의 Step 둘"
                            + "(purgeVerificationRunsStep · purgeBatchMetadataStep)은 하트비트가 "
                            + "청크 커밋에만 뛰어서, 그 사이 침묵을 시체로 읽으면 만료가 "
                            + "검증 한복판을 지나가거나 살아 있는 정리가 시체로 신고됩니다. "
                            + "stuck-job-after-ms=" + stuckAfterMs
                            + " 가장긴Step=batch." + longestName + ".step-timeout-ms("
                            + longestSilentStep + ")");
        }
        this.jobRepository = jobRepository;
        this.stuckAfter = Duration.ofMillis(stuckAfterMs);
    }

    /**
     * 막아야 하는 실행들. 없으면 빈 목록이다.
     *
     * <p>id 오름차순으로 준다. 호출자가 메시지에 싣는 값이라 순서가 흔들리면 같은 상황에서
     * 다른 문구가 나온다.
     */
    public List<Long> blockingExecutions(String jobName) {
        // 기준 시계는 LocalDateTime.now() 다. TimeProvider(Clock.systemUTC) 가 아니다 —
        // 배치 메타의 시각을 찍는 것이 인자 없는 LocalDateTime.now(), 즉 **JVM 기본
        // 타임존**이기 때문이다. START_TIME 은 AbstractJob 이, StepExecution.LAST_UPDATED 는
        // SimpleJobRepository.update 가 각각 그것으로 찍는다(6.0.4 바이트코드로 확인).
        // 컨테이너는 둘 다 UTC 라 지금은 일치하지만, KST 기기에서 bootRun 으로 띄우면
        // 모든 실행이 아홉 시간 늙은 것으로 보여 **가드가 통째로 꺼진다.**
        //
        // docs/04 는 배치 코드의 now() 를 금지한다. 여기가 그 유일한 예외이고,
        // NoWallClockInBatchTest 가 그 예외 목록을 못 박아 둔다.
        LocalDateTime now = LocalDateTime.now();

        return jobRepository.findRunningJobExecutions(jobName).stream()
                .filter(execution -> isAlive(jobName, execution, now))
                .map(JobExecution::getId)
                .sorted(Comparator.naturalOrder())
                .toList();
    }

    /**
     * <b>진도가 멈춘 채 남은 실행 하나.</b> {@code JobExecution} 만 주면 부르는 쪽이
     * "마지막 진도" 를 다시 계산해야 하는데, <b>그 폴백이 갈리면 판정과 표시가 다른 컬럼을
     * 가리킨다</b> — 실제로 그렇게 썼다가 리뷰가 잡았다. 판정에 쓴 값을 그대로 싣는다.
     *
     * <p>{@code stalledSeconds} 도 여기서 낸다. 배치 메타의 시각은 프레임워크가 <b>JVM 기본
     * 존</b>으로 찍으므로({@link #blockingExecutions} 의 근거) 그 뺄셈은 이 파일의 좌표계에서
     * 해야 한다. 부르는 쪽으로 넘기면 벽시계 예외가 한 파일 더 늘어난다.
     */
    public record StuckRun(JobExecution execution, LocalDateTime lastProgress,
            long stalledSeconds, LocalDateTime stuckBefore) {
    }

    /**
     * <b>진도가 멈춘 채 남은 실행들.</b> {@link #stuckExecutionCount} 가 세는 것과
     * <b>같은 판정</b>이고, 그것을 수(數)가 아니라 목록으로 낸다.
     *
     * <p>{@code BatchStuckExecution} 알림이 <i>"몇 건 있다"</i> 까지만 말해 준다. 그다음
     * <b>어느 실행인지</b>와 <b>진짜 죽었는지</b>를 사람이 봐야 하는데, 그 길이 지금까지
     * {@code docs/13} 의 손 SQL 뿐이었다 — 그 SQL 은 살아 있는 실행을 함께 뽑지 않으려고
     * 임계를 직접 적어야 했고, <b>그 숫자가 이 클래스와 갈리는 순간 운영자가 살아 있는
     * 잡을 걷어낸다.</b> 여기서 내면 임계가 한 곳에 남는다.
     *
     * <p>추가 질의는 없다 — {@code findRunningJobExecutions} 가 StepExecution 을 채워서
     * 주므로 마지막 진도까지 이 객체 안에 있다.
     */
    public List<StuckRun> stuckExecutions(String jobName) {
        LocalDateTime now = LocalDateTime.now();
        return jobRepository.findRunningJobExecutions(jobName).stream()
                .filter(execution -> isStuck(jobName, execution, now))
                .sorted(Comparator.comparing(JobExecution::getId))
                .map(execution -> {
                    LocalDateTime progress = lastProgress(execution);
                    return new StuckRun(execution, progress,
                            Duration.between(progress, now).toSeconds(),
                            now.minus(stuckAfter));
                })
                .toList();
    }

    /**
     * 진도가 멈춘 채 실행 중으로 남은 행의 수 — <b>가드가 무시하기로 한 것들</b>이다.
     *
     * <p>가드가 그것을 무시할 때 남기는 것은 WARN 로그뿐인데, 로그는 감시 수단이 아니다.
     * 게다가 <b>가드 경로에서만</b> 나온다 — 검증을 하루 한 번 돌리면 그 사이 며칠은 아무
     * 신호가 없다. 그래서 같은 판정을 지표로도 낸다. <b>여기서는 로그를 안 남긴다</b> —
     * 되읽기 주기마다 같은 줄이 쌓이면 그것이 다른 로그를 밀어낸다.
     *
     * <p>임계와 판정 규칙을 이 클래스에 두는 이유는 하나다 — 두 곳에 적으면 갈린다.
     *
     * <p>목록이 필요하면 {@link #stuckExecutions} 다 — 같은 판정이다.
     */
    public int stuckExecutionCount(String jobName) {
        // blockingExecutions 와 같은 이유로 LocalDateTime.now() 다 — 배치 메타의 시각이
        // 그 좌표계로 찍히기 때문이고, TimeProvider(UTC)로 바꾸면 KST 기기에서 아홉 시간
        // 어긋나 판정이 통째로 뒤집힌다. NoWallClockInBatchTest 가 이 파일을 예외로 둔다.
        LocalDateTime now = LocalDateTime.now();
        return (int) jobRepository.findRunningJobExecutions(jobName).stream()
                .filter(execution -> isStuck(jobName, execution, now))
                .count();
    }

    /**
     * <b>나이가 아니라 진도로 판정한다.</b>
     *
     * <p>배치 메타에는 종료 표시를 못 남기고 죽은 실행이 {@code STARTED} 로 <b>영원히</b>
     * 남는다 — 위 질의에 {@code END_TIME} 검사도 시간 상한도 없기 때문이다. 그래서 살았는지
     * 죽었는지를 따로 물어야 하는데, <b>시작한 지 오래됐다는 것은 그 답이 아니다.</b>
     * 오래 도는 것과 죽은 것은 다른 사건이고, 나이로 자르면 <b>느린 만료를 죽었다고 판정해
     * 가드가 가장 필요한 순간에 스스로 꺼진다.</b>
     *
     * <p><b>하트비트가 그 답이다.</b> {@code SimpleJobRepository.update(StepExecution)} 이
     * 청크 커밋마다 {@code LAST_UPDATED} 를 {@code LocalDateTime.now()} 로 다시 찍는다
     * (6.0.4 바이트코드로 확인). 살아 있는 잡은 이 값이 계속 앞으로 가고, 하드킬된 잡은
     * 죽은 순간에 멈춘다. {@code expireStep} 은 {@code RepeatStatus.CONTINUABLE} 로 청크마다
     * 트랜잭션을 끊으므로 이 값이 실제로 움직인다.
     *
     * <p><b>{@code verifyJob} 쪽은 사정이 다르다.</b> Step 열하나 중 청크는
     * {@code replayStep} 뿐이고 나머지 열은 단발 태스클릿이라, 하트비트가 <b>Step 경계에서만</b>
     * 뛴다. 그래서 무진도 구간의 상한이 {@code batch.verify.step-timeout-ms} 이고,
     * 생성자가 그 관계를 기동 때 검사한다.
     *
     * <p>추가 질의는 없다 — {@code SimpleJobExplorer.findRunningJobExecutions} 가
     * {@code fillStepExecutionDependencies} 까지 돌려 <b>StepExecution 을 채워서 준다</b>
     * (같은 방법으로 확인).
     *
     * <p>⚠️ <b>양쪽에서 눌린다.</b> 아래로는 {@code BatchJobRunningTooLong}(600초)과
     * {@code CleanupRunningTooLong}(900초)보다 커야 한다 — <b>그쪽은 손으로 맞춘다.</b>
     * 알림 임계는 프로메테우스에 있어 앱이 못 읽으므로 검사하는 코드가 없다.
     * 아래 생성자가 보는 것은 <b>위쪽 관계뿐</b>이다. 그 알림들이 읽는
     * {@code spring_batch_job_active_seconds_max} 는 JVM 안의 게이지라, 그것이 우는 상태는
     * 정의상 <b>"살아서 느리게 돌고 있다"</b>이다.
     * 위로는 <b>가장 긴 Step 데드라인보다 커야 한다</b> — {@code batch.verify}·
     * {@code batch.expire}·{@code batch.cleanup} 의 {@code step-timeout-ms} <b>셋 다</b>이고,
     * 생성자가 그 셋을 인자로 받아 검사한다(잡이 늘면 인자가 는다).
     * 어느 쪽이든 붙여 놓으면 <i>"살아 있다"</i>와 <i>"죽었다"</i>가 같은 숫자를 쓴다 —
     * 한때 아래쪽에서, 그다음 위쪽에서 같은 사고를 냈다.
     */
    private boolean isAlive(String jobName, JobExecution execution, LocalDateTime now) {
        if (lastProgress(execution) == null) {
            // 여기 오면 배치 메타가 계약을 깬 것이다 — CREATE_TIME 은 NOT NULL 이라
            // 실제로는 도달하지 않는다. 모를 때는 막는 쪽으로 기운다.
            log.warn("{} 실행의 시각이 전부 비어 있어 도는 중으로 봅니다. executionId={}",
                    jobName, execution.getId());
            return true;
        }
        if (!isStuck(jobName, execution, now)) {
            return true;
        }
        // **로그는 가드 경로에서만 남긴다.** 되읽기(BatchRunMetricsRefresher)도 같은 판정을
        // 쓰는데, 거기서 찍으면 시체 행 하나가 남은 것만으로 WARN 이 하루 1,440줄 나온다 —
        // 그러면 알림 description 이 가리키는 "만료 슬롯을 건너뜁니다" WARN 이 그 안에 묻힌다.
        // 되읽기 쪽은 로그 없이 같은 사실을 지표로 낸다.
        log.warn("{} 실행의 진도가 {}초째 멈춰 있어 무시합니다. 종료 표시를 못 남기고 죽은 "
                        + "실행일 수 있습니다 — BATCH_JOB_EXECUTION 과 BATCH_STEP_EXECUTION 을 "
                        + "확인하십시오. executionId={} 마지막진도={}",
                jobName, stuckAfter.toSeconds(), execution.getId(), lastProgress(execution));
        return false;
    }

    /**
     * 진도가 멈췄는가 — <b>판정만 한다.</b> 로그는 부르는 쪽이 정한다.
     *
     * <p>가드와 되읽기가 같은 판정을 써야 지표와 동작이 갈리지 않는데, 로깅까지 같이 하면
     * 되읽기 주기마다 같은 줄이 나온다. 그래서 판정과 로깅을 여기서 가른다.
     *
     * <p><b>{@code jobName} 은 안 쓴다.</b> 시그니처에 남겨 둔 것은 호출부가 무엇을 재는지
     * 읽히게 하기 위해서다 — 로그가 이 메서드를 떠났으므로 값 자체는 필요 없다.
     */
    private boolean isStuck(String jobName, JobExecution execution, LocalDateTime now) {
        LocalDateTime since = lastProgress(execution);
        // 시각을 하나도 못 구한 행은 "멈췄다" 로 안 센다 — 모를 때는 막는 쪽이고,
        // 그 판단의 로그는 가드 경로(isAlive)가 남긴다. 여기서 찍으면 되읽기 주기마다
        // 같은 줄이 쌓여, 이 클래스가 막으려던 로그 홍수가 다른 갈래로 되살아난다.
        return since != null && Duration.between(since, now).compareTo(stuckAfter) > 0;
    }

    /**
     * 마지막 진도. Step 이 아직 안 시작했으면 실행 시작 시각으로, 그것도 없으면
     * 생성 시각으로 물러난다.
     *
     * <p>{@code STARTING} 에서 죽어 Step 도 {@code START_TIME} 도 없는 행이 있는데,
     * 그 갈래가 없으면 그런 행이 <b>영원히</b> 막는다. 그 행은 CY-429 의 복구 API 로 걷는다.
     */
    private static LocalDateTime lastProgress(JobExecution execution) {
        return execution.getStepExecutions().stream()
                .map(StepExecution::getLastUpdated)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder())
                .orElseGet(() -> execution.getStartTime() != null
                        ? execution.getStartTime()
                        : execution.getCreateTime());
    }
}

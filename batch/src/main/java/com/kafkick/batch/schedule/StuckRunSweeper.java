// 진도가 멈춘 실행을 주기적으로 걷어냅니다.
package com.kafkick.batch.schedule;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.kafkick.batch.api.StuckRunSweepService;
import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.batch.config.RunningJobProbe.StuckRun;

/**
 * <b>탐지는 세 겹이었는데 조치가 없었다.</b> {@link RunningJobProbe#stuckExecutions} 가
 * 판정하고, {@code GET /runs/stuck} 이 보여 주고, {@code BatchStuckExecution} 이 10분 뒤
 * 울린다 — 그리고 <b>거기서 끝이었다.</b> 회수 경로 넷({@code ExpireRecoveryService} ·
 * {@code CleanupRecoveryService} · {@code VerifyStopService} ·
 * {@code BatchRunAbandonService})의 호출자를 전수로 세면 전부 컨트롤러다.
 *
 * <p>배치 JVM 이 하드킬로 죽은 새벽에는 아무도 안 누른다. 사전예약 PRD FR-C-01 의 수용
 * 기준이 <i>"처리가 정체된 건이 <b>자동으로</b> 해소됩니다"</i> 인 것이 이 자리다.
 *
 * <h2>⚠️ 시체가 막는 것이 무엇인지 재고 좁혔다</h2>
 *
 * <p>처음에는 <i>"그 행이 다음 회차를 {@code VERIFY_ALREADY_RUNNING}·
 * {@code VERIFY_EXPIRE_RUNNING} 으로 계속 밀어낸다"</i> 고 적었는데 <b>뒤엣것이 틀렸다.</b>
 * {@code VERIFY_EXPIRE_RUNNING} 은 {@link RunningJobProbe#blockingExecutions} 를 쓰고, 그것은
 * {@code isAlive} 로 거르는데 <b>{@code isAlive} 는 시체를 거짓으로 뺀다</b> — 임계를 넘는
 * 순간, 즉 이 스윕이 손댈 수 있게 되는 바로 그 시점부터 그 코드가 그 행을 무시한다.
 * {@code docs/13} 이 그 사실을 <i>"상호 배제가 그 실행에 대해 <b>꺼져</b> 있다"</i> 로
 * 정확히 적어 뒀다 — <b>밀어내는 것이 아니라 눈이 머는 것</b>이다.
 *
 * <p>영구히 막히는 것은 <b>{@code POST /api/v1/admin/verify} 하나</b>다.
 * {@code rejectIfAlreadyRunning} 은 {@code findRunningJobExecutions} 를 날것으로 쓰고
 * 그 주석이 <i>"하드킬로 남은 행도 여기 잡힌다"</i> 고 적어 뒀다. 크론 검증은 {@code asOf} 가
 * 매일 달라 안 막힌다. 잡을 도입할 이유로는 충분하지만, <b>근거는 그만큼만이다.</b>
 *
 * <h2>살아 있는 잡을 안 건드리는 근거는 이 클래스가 아니다</h2>
 *
 * <p>판정을 여기서 다시 짜지 않는다 — {@link RunningJobProbe#stuckExecutions} 하나를 쓴다.
 * {@code /runs/stuck} 과 스윕이 다른 것을 시체라고 부르면 <b>화면이 보여 주지 않은 것을
 * 스윕이 걷는다.</b> 그 판정은 <b>마지막 진도 시각</b>을 보고, 임계가 가장 긴 무진도 구간보다
 * 큰지를 {@code RunningJobProbe} 생성자가 <b>기동에서</b> 강제한다.
 *
 * <p>그 위에 선점문이 한 겹 더 있다. 판정과 쓰기 사이에 잡이 되살아날 수 있어
 * ({@code StuckRunClaim} 이 적어 둔 사실: 락 대기가 풀리는 경우가 그렇다) 같은 조건을
 * {@code UPDATE} 에 다시 걸고 affected rows 를 본다.
 *
 * <h2>스위치가 둘인 이유 — 하나로는 손이 안 닿았다</h2>
 *
 * <p>{@code batch.scheduling.enabled} 하나로 두려다 <b>되돌렸다.</b> 그 스위치는 스케줄러
 * <b>다섯</b>을 함께 무는데 그중 {@code CouponRoundScheduler} 는 1분마다 회차를 열고 닫는
 * <b>사용자 대면 동작</b>이라, 운영 중에는 쓸 수 없다.
 *
 * <p>대안으로 적었던 <i>"{@code batch.stuck-job-after-ms} 를 올린다"</i> 도 <b>틀렸다.</b>
 * 그 임계 하나가 지표({@code cy_batch_stuck_executions})와 {@code /runs/stuck} 목록과
 * <b>사람이 부르는 {@code recover} 의 {@code requireStuck} 관문까지</b> 전부 구동한다 —
 * 올리면 조치만 꺼지는 것이 아니라 <b>알림이 조용해지고, 목록이 비고, 수동 회수가
 * {@code EXPIRE_EXECUTION_NOT_STUCK} 으로 거절된다.</b> 게다가 재기동이 필요하다.
 *
 * <p>그래서 {@code batch.stuck-sweep.enabled} 를 따로 둔다. 이 저장소의 규칙
 * (<i>"끌 것이 여러 개면 하나는 반드시 빠뜨린다"</i>)은 <b>알림으로 갚는다</b> — 꺼도
 * {@code BatchStuckExecution} 은 그대로 울고, {@code cy_batch_stuck_sweep_enabled} 가
 * 그때 <b>왜 아무도 안 걷는지</b>를 말한다. 끈 것을 알림이 모르는 상태가 진짜 사고다.
 *
 * <p><b>그 키를 {@code @ConditionalOnProperty} 에 안 넣는다 — {@code @Value} 다.</b>
 * 조건에 넣으면 {@code matchIfMissing = false} 라 <b>키가 없는 형상에서 빈이 통째로
 * 안 선다</b>: 조용히 안 도는 상태이고, 그것이 이 티켓이 없앤 바로 그 상태다.
 * 실제로 그 실수를 한 번 했다 — 테스트 리소스 {@code application.yml} 이
 * {@code .example} 을 가려서 {@code SchedulerPoolGuardTest} 가 <i>"등록 11, 기본값 12"</i>
 * 로 잡았다. {@code @Value} 면 기본이 <b>켜짐</b>이라 빠뜨려도 조치가 산다.
 *
 * <p>그 대가로 <b>꺼도 스케줄 작업 하나는 등록된다</b>(즉시 반환한다). 풀 크기를 세는
 * {@code SchedulerPoolGuard} 기준으로는 그것이 맞는 답이다 — 등록된 것은 등록된 것이다.
 */
@Component
@ConditionalOnProperty(name = "batch.scheduling.enabled", havingValue = "true",
        matchIfMissing = false)
public class StuckRunSweeper {

    private static final Logger log = LoggerFactory.getLogger(StuckRunSweeper.class);

    private final StuckRunSweepService sweep;
    private final RunningJobProbe runningJobs;
    private final List<String> jobNames;
    private final int maxPerSweep;
    private final boolean enabled;
    private final TransactionTemplate readStuck;

    /**
     * <b>이름 순으로 정렬한다 — 빈 순서는 보장이 없다.</b> 상한에 걸려 일부만 걷는 날
     * <i>어느</i> 잡이 먼저인지가 실행마다 달라지면 남은 것을 추적할 수 없다.
     * {@code BatchHistoryController} 가 같은 이유로 같은 모양을 쓴다.
     *
     * <p><b>조회에 데드라인을 건다.</b> 한때 그것이 있다고 주석에 적었는데 <b>없었다</b> —
     * {@code stuckExecutions} 는 {@code JobRepository} 프록시의 기본 트랜잭션으로 돌아
     * 유일한 상한이 JDBC {@code socketTimeout} 이었고, 그것은
     * {@code DataSourceTimeoutGuard} 가 가장 긴 Step 데드라인보다 크게 강제한다.
     * 배치 메타가 물리면 <b>이 클래스가 막겠다고 선언한 "조용히 안 도는 상태"</b> 로
     * 그대로 들어간다. 형제 둘이 이미 감싸고 있다 — 컨트롤러는 {@code @Transactional},
     * 되읽기는 타임아웃 건 {@code TransactionTemplate}. 뒤엣것을 따른다.
     *
     * <p><b>루프 전체를 감싸면 안 된다.</b> {@code recover} 가 {@code REQUIRED} 라 그 하나에
     * 합류해 버리고, 그러면 락 보유 구간이 스윕 전체로 늘어 손으로 걷으려는 운영자가
     * 그동안 락 대기에 걸린다.
     *
     * @param maxPerSweep 한 주기에 걷을 상한. 메타 전체가 시체인 상황에서 한 주기가
     *                    수백 건을 트랜잭션마다 열지 않게 한다 — 남은 것은 다음 주기가
     *                    가져가고, 그 사이 {@code BatchStuckExecution} 은 계속 울린다
     */
    public StuckRunSweeper(StuckRunSweepService sweep, RunningJobProbe runningJobs,
            List<Job> jobs, PlatformTransactionManager transactionManager,
            @Value("${batch.stuck-sweep.max-per-sweep:20}") int maxPerSweep,
            @Value("${batch.stuck-sweep.enabled:true}") boolean enabled,
            @Value("${batch.admin.timeout-seconds:5}") int readTimeoutSeconds) {
        if (maxPerSweep < 1) {
            // 0 으로 끄는 길을 안 연다. 그러면 스케줄러는 도는데 아무것도 안 하는 상태가
            // 되고, cy_batch_stuck_sweep_enabled 가 1 이라 알림이 "무장돼 있다" 고 말한다 —
            // 끄는 손잡이는 batch.stuck-sweep.enabled 쪽이고 그것은 게이지에 잡힌다.
            throw new IllegalArgumentException(
                    "batch.stuck-sweep.max-per-sweep 는 1 이상이어야 합니다. 스윕을 끄려면 "
                            + "batch.stuck-sweep.enabled=false 를 쓰십시오 — 그쪽은 "
                            + "cy_batch_stuck_sweep_enabled 로 관제에 보입니다. 받은 값="
                            + maxPerSweep);
        }
        this.sweep = sweep;
        this.runningJobs = runningJobs;
        this.jobNames = jobs.stream().map(Job::getName).sorted().toList();
        this.maxPerSweep = maxPerSweep;
        this.enabled = enabled;
        this.readStuck = new TransactionTemplate(transactionManager);
        this.readStuck.setReadOnly(true);
        this.readStuck.setTimeout(readTimeoutSeconds);
    }

    /**
     * <b>예외를 밖으로 던지지 않는다.</b> {@code @Scheduled} 에서 예외가 나가면 스프링이
     * 로그만 남기고 다음 주기를 잡는데, 그러면 스윕이 <b>조용히 안 도는 상태</b>가 된다.
     * 형제 넷이 같은 이유로 같은 모양을 쓴다.
     *
     * <p><b>{@code fixedDelay} 다.</b> {@code fixedRate} 로 두면 앞 주기가 락 대기에 걸린
     * 동안 다음 주기가 겹쳐 뜨고, 둘이 같은 실행을 선점하려고 다툰다 — 선점문이 답을
     * 가르기는 하지만 그 다툼 자체가 배치 메타에 락을 더 건다.
     */
    @Scheduled(fixedDelayString = "${batch.stuck-sweep.interval-ms:60000}")
    public void sweep() {
        if (!enabled) {
            return;
        }
        int budget = maxPerSweep;
        int closed = 0;
        for (String jobName : jobNames) {
            // **이 검사가 지는 것은 정정이 아니라 질의다.** 지워도 아래 안쪽 검사가
            // 곧바로 끊어 결과는 같다(돌연변이로 확인했다). 여기 있는 이유는
            // stuckExecutions 가 실행마다 DAO 세 번, Step 마다 한 번을 더 부르는 비싼
            // 조회라서다 — 예산이 0 인데 남은 잡마다 그것을 부르는 것은 배치 메타가
            // 이미 아픈 날에 부담만 더한다. 그래서 테스트도 결과가 아니라 호출을 잰다.
            if (budget == 0) {
                warnCapped(jobName);
                break;
            }
            List<StuckRun> stuckRuns;
            try {
                stuckRuns = readStuck.execute(status -> runningJobs.stuckExecutions(jobName));
            } catch (Exception e) {
                sweep.recordFailure();
                log.error("시체 조회가 끊겼습니다. 나머지 잡은 계속 봅니다. jobName={}",
                        jobName, e);
                continue;
            }
            for (StuckRun stuck : stuckRuns) {
                if (budget == 0) {
                    // **안쪽에서도 말해야 한다.** 이름 순 마지막 잡에서 예산이 떨어지면
                    // 바깥 검사는 한 번도 안 타고, 그러면 상한에 걸린 주기가 한 줄도
                    // 안 남는다 — 시체가 몰리는 것은 대개 한 잡이라 그쪽이 흔한 경우다.
                    warnCapped(jobName);
                    break;
                }
                budget--;
                long executionId = stuck.execution().getId();
                try {
                    if (sweep.recover(executionId, stuck.stuckBefore())) {
                        // **지표는 여기서 안 센다.** 이 빈은 조건부라 꺼진 형상에서는
                        // 미터가 아예 안 태어난다 — increase() 가 0 이 아니라 빈 결과가
                        // 되어 알림이 영원히 안 운다. 서비스가 커밋 뒤에 든다.
                        closed++;
                    }
                } catch (Exception e) {
                    // **건 단위로 삼킨다.** 잡 단위로 감싸면 이 한 건이 그 잡의 나머지
                    // 시체를 전부 이번 주기에서 밀어내는데, 목록이 id 오름차순이고 롤백으로
                    // 선점이 되돌아가므로 다음 주기도 같은 자리에서 끊긴다 —
                    // **id 가 큰 시체가 영구히 안 걷힌다.**
                    sweep.recordFailure();
                    log.error("시체 하나를 걷지 못했습니다. 나머지는 계속 봅니다. "
                            + "jobName={} executionId={}", jobName, executionId, e);
                }
            }
        }
        if (closed > 0) {
            log.warn("시체 스윕이 실행 {}건을 FAILED 로 닫았습니다.", closed);
        }
    }

    /** 상한 경고는 두 자리에서 나오므로 문구를 한 곳에 둔다 — 갈리면 검색이 안 걸린다. */
    private void warnCapped(String jobName) {
        log.warn("시체 스윕 상한에 걸려 이번 주기를 여기서 끊습니다. 남은 것은 다음 주기가 "
                + "가져갑니다. 상한={} 멈춘잡={}", maxPerSweep, jobName);
    }
}

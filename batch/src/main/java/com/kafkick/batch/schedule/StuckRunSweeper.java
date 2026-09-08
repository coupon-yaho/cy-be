// 진도가 멈춘 실행을 주기적으로 걷어냅니다.
package com.kafkick.batch.schedule;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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
 * 울린다 — 그리고 <b>거기서 끝이었다.</b> 회수 경로 다섯({@code ExpireRecoveryService} ·
 * {@code CleanupRecoveryService} · {@code VerifyStopService} ·
 * {@code VerifyAbandonService} · {@code BatchRunAbandonService})의 호출자를 전수로 세면
 * 전부 컨트롤러다.
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

    /**
     * {@code BatchStuckExecution} 의 {@code for} 다. <b>손으로 맞춘다</b> — 규칙 파일을
     * 코드가 읽을 수 없어서이고, {@code batch.metrics.*-running-too-long-seconds} 셋이
     * 같은 이유로 같은 모양이다. 규칙의 값을 바꾸면 여기도 함께 옮긴다.
     */
    private static final Duration ALERT_WINDOW = Duration.ofMinutes(10);

    private final StuckRunSweepService sweep;
    private final RunningJobProbe runningJobs;
    private final List<String> jobNames;
    private final int maxPerSweep;
    private final boolean enabled;
    private final TransactionTemplate readStuck;

    /**
     * <b>어느 잡부터 볼지.</b> {@code @Scheduled} 는 이 빈에 대해 단일 스레드라 동기화가
     * 필요 없지만({@code ExpireScheduler} 가 같은 사실을 적어 뒀다), 손 호출을 섞는
     * 테스트가 있어 원자형으로 든다.
     */
    private final AtomicInteger startAt = new AtomicInteger();

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
            @Value("${batch.stuck-sweep.enabled:true}") String enabled,
            @Value("${batch.stuck-sweep.interval-ms:60000}") long intervalMillis,
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
        // **주기와 알림을 한자리에서 맞춘다.** BatchStuckExecution 이 for: 10m 이라,
        // 주기가 그보다 길면 시체가 **한 번도 안 걷힌 채** 그 알림이 뜬다 — 그런데 그
        // 알림의 진단문은 "게이지도 1이고 실패도 없으면 실행이 매 주기 되살아나는 것"
        // 이라고 말한다. 설정이 그 말을 거짓으로 만드는 상태를 기동에서 끊는다.
        // CleanupScheduler 가 크론과 SLA 를 같은 방식으로 맞춘다.
        if (intervalMillis < 1 || intervalMillis > ALERT_WINDOW.toMillis() / 2) {
            throw new IllegalArgumentException(
                    "batch.stuck-sweep.interval-ms 는 1 이상이면서 "
                            + ALERT_WINDOW.toMillis() / 2 + " 이하여야 합니다. "
                            + "BatchStuckExecution 이 " + ALERT_WINDOW.toMinutes()
                            + "분에 뜨는데 그 안에 스윕이 최소 두 번은 돌아야 "
                            + "'아무도 안 걷고 있다' 가 사실이 됩니다. 받은 값=" + intervalMillis);
        }
        this.sweep = sweep;
        this.runningJobs = runningJobs;
        this.jobNames = jobs.stream().map(Job::getName).sorted().toList();
        this.maxPerSweep = maxPerSweep;
        // **게이지와 같은 규칙으로 읽는다.** 여기만 @Value boolean 으로 두면
        // StringToBooleanConverter 가 1·yes·on 을 참으로 봐서, 스윕은 도는데
        // cy_batch_stuck_sweep_enabled 가 0 인 상태가 난다.
        this.enabled = StuckRunSweepService.isOn(enabled);
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
        long notAttempted = 0;
        for (String jobName : rotated()) {
            // **예산이 0 이어도 남은 잡을 센다.** 한때 여기서 끊었는데, 그러면 앞 잡이
            // 상한을 **정확히** 소진하고 뒤 잡에 시체가 남은 주기가 "용량 정상" 으로
            // 보인다 — 그 상태에서 BatchStuckExecution 이 뜨면 진단문이 남은 갈래
            // ("실행이 매 주기 되살아난다")를 지목해 운영자가 임계를 만지러 간다.
            //
            // 아끼는 것은 잡 수만큼의 조회다(지금 셋이면 최대 둘). 그 값과 **적체를
            // 정확히 아는 것**을 맞바꿀 이유가 없다. 상한이 막는 것은 **쓰기**이고,
            // 그것은 아래 taken 이 그대로 진다.
            List<StuckRun> stuckRuns;
            try {
                stuckRuns = readStuck.execute(status -> runningJobs.stuckExecutions(jobName));
            } catch (Exception e) {
                sweep.recordFailure();
                log.error("시체 조회가 끊겼습니다. 나머지 잡은 계속 봅니다. jobName={}",
                        jobName, e);
                continue;
            }
            // **남긴 수를 추측하지 않고 뺀다.** 증거 없이 세면 예산이 딱 맞아떨어진
            // 주기에도 카운터가 올라 알림이 멀쩡한 용량을 지목하고, 안 세면 반대로
            // 진짜 적체를 놓친다. 본 목록에서 **고른** 만큼을 빼면 둘 다 안 난다.
            //
            // ⚠️ **"고른" 이지 "걷은" 이 아니다.** 아래에서 recover 가 던진 건도 taken 에
            // 들어가 있어 여전히 시체로 남는다 — 그 축은 recordFailure 가 따로 진다.
            // 둘을 한 수에 합치면 "상한을 올리십시오" 와 "왜 못 걷는지 보십시오" 라는
            // 서로 다른 처방이 섞인다. 이 값이 답하는 질문은 **"상한이 얼마나 모자랐나"**
            // 하나다.
            int taken = Math.min(budget, stuckRuns.size());
            notAttempted += stuckRuns.size() - taken;
            for (int i = 0; i < taken; i++) {
                StuckRun stuck = stuckRuns.get(i);
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
        // **끝까지 돈 주기마다 적는다 — 0 도 적는다.** 남았을 때만 적으면 상한을 올려
        // 해소한 뒤에도 게이지가 옛 값으로 굳어 관제가 계속 밀려 있다고 말한다.
        //
        // ⚠️ **꺼진 주기는 여기 안 온다** — 위 조기 반환이 먼저다. 그때 게이지는 마지막
        // 값을 그대로 든다. 0 으로 내리는 것이 오히려 거짓이기 때문이다: 아무도 안 걷는
        // 동안 그 적체는 실제로 남아 있다. 왜 안 줄어드는지는
        // cy_batch_stuck_sweep_enabled 가 0 으로 말한다.
        sweep.recordBacklog(notAttempted);
        if (notAttempted > 0) {
            log.warn("시체 스윕 상한에 걸려 {}건을 이번 주기에 고르지도 못했습니다. "
                    + "다음 주기는 다른 잡부터 봅니다. 상한={}", notAttempted, maxPerSweep);
        }
        if (closed > 0) {
            log.warn("시체 스윕이 실행 {}건을 FAILED 로 닫았습니다.", closed);
        }
    }

    /**
     * <b>매 주기 시작 잡을 한 칸 민다.</b> 고정 순서 + 전역 상한이면 <b>앞 잡이 상한 이상을
     * 계속 내는 동안 뒤 잡은 영원히 조회조차 안 된다</b> — 그 잡의 시체는 자동 회수의
     * 대상에서 빠지고, 그 사실을 아무도 말해 주지 않는다(예산이 0 이라 조회를 건너뛰므로
     * 지표에도 안 잡힌다).
     *
     * <p>한 칸씩만 민다. 무작위로 섞으면 <b>상한에 걸린 날 어느 잡이 남았는지 추적할 수
     * 없다</b> — 이름 순 정렬을 유지한 이유가 그것이고, 회전은 그 순서를 안 깬다.
     */
    private List<String> rotated() {
        if (jobNames.isEmpty()) {
            return jobNames;
        }
        int offset = Math.floorMod(startAt.getAndIncrement(), jobNames.size());
        List<String> order = new ArrayList<>(jobNames.size());
        for (int i = 0; i < jobNames.size(); i++) {
            order.add(jobNames.get((offset + i) % jobNames.size()));
        }
        return order;
    }
}

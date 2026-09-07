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
 * <p>배치 JVM 이 하드킬로 죽은 새벽에는 아무도 안 누른다. 그동안 그 행 하나가
 * {@code STARTED} 로 남아 다음 회차를 {@code VERIFY_ALREADY_RUNNING} ·
 * {@code VERIFY_EXPIRE_RUNNING} 으로 계속 밀어낸다. 사전예약 PRD FR-C-01 의 수용 기준이
 * <i>"처리가 정체된 건이 <b>자동으로</b> 해소됩니다"</i> 인 것이 이 자리다.
 *
 * <h2>살아 있는 잡을 안 건드리는 근거는 이 클래스가 아니다</h2>
 *
 * <p>판정을 여기서 다시 짜지 않는다 — {@link RunningJobProbe#stuckExecutions} 하나를 쓴다.
 * {@code /runs/stuck} 과 스윕이 다른 것을 시체라고 부르면 <b>화면이 보여 주지 않은 것을
 * 스윕이 걷는다.</b> 그 판정은 <b>마지막 진도 시각</b>을 보고, 임계가 가장 긴 무진도 구간보다
 * 큰지를 {@code RunningJobProbe} 생성자가 <b>기동에서</b> 강제한다 — 도는 잡이 잠깐 조용한
 * 것과 죽은 것을 가르는 것이 그 관문이다.
 *
 * <p>그 위에 선점문이 한 겹 더 있다. 판정과 쓰기 사이에 잡이 되살아날 수 있어
 * ({@code StuckRunClaim} 이 적어 둔 사실: 락 대기가 풀리는 경우가 그렇다) 같은 조건을
 * {@code UPDATE} 에 다시 걸고 affected rows 를 본다.
 *
 * <h2>스위치를 따로 두지 않는다</h2>
 *
 * <p>{@code batch.scheduling.enabled} 하나를 형제 넷과 같이 쓴다. 이 저장소가 이미 정한
 * 규칙이고({@code CleanupScheduler}: <i>"끌 것이 여러 개면 하나는 반드시 빠뜨린다"</i>)
 * 근거가 여기에도 그대로 선다 — 스윕만 따로 끄면 <b>탐지·알림은 그대로 울리는데 조치만
 * 꺼진 상태</b>가 되고, 그것이 정확히 이 티켓이 없앤 상태다.
 *
 * <p>디버깅하려고 시체를 남겨 두고 싶으면 {@code batch.stuck-job-after-ms} 를 올린다 —
 * 판정을 늦추는 것이 조치를 끄는 것보다 정직하다. 그리고 이 스윕이 하는 것은
 * {@code FAILED} 로 닫는 것뿐이라 <b>이력도 재시작 가능성도 안 잃는다</b>
 * ({@link StuckRunSweepService} 가 {@code abandon} 을 안 쓰는 이유).
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

    /**
     * <b>이름 순으로 정렬한다 — 빈 순서는 보장이 없다.</b> 상한에 걸려 일부만 걷는 날
     * <i>어느</i> 잡이 먼저인지가 실행마다 달라지면 남은 것을 추적할 수 없다.
     * {@code BatchHistoryController} 가 같은 이유로 같은 모양을 쓴다.
     *
     * @param maxPerSweep 한 주기에 걷을 상한. 메타 전체가 시체인 상황에서 한 주기가
     *                    수백 건을 트랜잭션마다 열지 않게 한다 — 남은 것은 다음 주기가
     *                    가져가고, 그 사이 {@code BatchStuckExecution} 은 계속 울린다
     */
    public StuckRunSweeper(StuckRunSweepService sweep, RunningJobProbe runningJobs,
            List<Job> jobs,
            @Value("${batch.stuck-sweep.max-per-sweep:20}") int maxPerSweep) {
        if (maxPerSweep < 1) {
            // 0 으로 끄는 길을 안 연다. 그러면 스케줄러는 도는데 아무것도 안 하는 상태가
            // 되고, 그것을 알림에 말해 주는 것이 아무것도 없다 — CleanupScheduler 가
            // 크론 "-" 를 거절하는 것과 같은 근거다.
            throw new IllegalArgumentException(
                    "batch.stuck-sweep.max-per-sweep 는 1 이상이어야 합니다. 스윕을 끄려면 "
                            + "batch.scheduling.enabled=false 를 쓰십시오. 받은 값="
                            + maxPerSweep);
        }
        this.sweep = sweep;
        this.runningJobs = runningJobs;
        this.jobNames = jobs.stream().map(Job::getName).sorted().toList();
        this.maxPerSweep = maxPerSweep;
    }

    /**
     * <b>예외를 밖으로 던지지 않는다.</b> {@code @Scheduled} 에서 예외가 나가면 스프링이
     * 로그만 남기고 다음 주기를 잡는데, 그러면 스윕이 <b>조용히 안 도는 상태</b>가 된다.
     * 형제 넷이 같은 이유로 같은 모양을 쓴다.
     *
     * <p><b>잡 하나가 던져도 나머지를 돈다.</b> 한 잡의 배치 메타 조회가 데드라인을 넘겨
     * 끊기는 것이 나머지 잡의 시체를 못 걷을 이유가 아니다.
     *
     * <p><b>{@code fixedDelay} 다.</b> {@code fixedRate} 로 두면 앞 주기가 락 대기에 걸린
     * 동안 다음 주기가 겹쳐 뜨고, 둘이 같은 실행을 선점하려고 다툰다 — 선점문이 답을
     * 가르기는 하지만 그 다툼 자체가 배치 메타에 락을 더 건다.
     */
    @Scheduled(fixedDelayString = "${batch.stuck-sweep.interval-ms:60000}")
    public void sweep() {
        int budget = maxPerSweep;
        int closed = 0;
        for (String jobName : jobNames) {
            // **이 검사가 지는 것은 정정이 아니라 질의다.** 지워도 아래 안쪽 검사가
            // 곧바로 끊어 결과는 같다(돌연변이로 확인했다). 여기 있는 이유는
            // stuckExecutions 가 실행마다 DAO 세 번, Step 마다 한 번을 더 부르는 비싼
            // 조회라서다 — 예산이 0 인데 남은 잡마다 그것을 부르는 것은 배치 메타가
            // 이미 아픈 날에 부담만 더한다. 그래서 테스트도 결과가 아니라 호출을 잰다.
            if (budget == 0) {
                log.warn("시체 스윕 상한에 걸려 이번 주기를 여기서 끊습니다. 남은 잡은 다음 "
                        + "주기가 가져갑니다. 상한={} 멈춘잡={}", maxPerSweep, jobName);
                break;
            }
            try {
                for (StuckRun stuck : runningJobs.stuckExecutions(jobName)) {
                    if (budget == 0) {
                        break;
                    }
                    budget--;
                    if (sweep.recover(stuck.execution().getId(), stuck.stuckBefore())) {
                        // **지표는 여기서 안 센다.** 이 빈은 batch.scheduling.enabled 조건부라
                        // 꺼진 형상에서는 미터가 아예 안 태어난다 — increase() 가 0 이 아니라
                        // 빈 결과가 되어 알림이 영원히 안 운다. 서비스가 든다.
                        closed++;
                    }
                }
            } catch (Exception e) {
                log.error("시체 스윕이 한 잡에서 끊겼습니다. 나머지 잡은 계속 봅니다. "
                        + "jobName={}", jobName, e);
            }
        }
        if (closed > 0) {
            log.warn("시체 스윕이 실행 {}건을 FAILED 로 닫았습니다. 재시작할 수 있습니다.", closed);
        }
    }
}

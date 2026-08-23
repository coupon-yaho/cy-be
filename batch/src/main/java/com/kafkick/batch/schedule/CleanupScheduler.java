// 정리 잡을 배치 창에 띄웁니다.
package com.kafkick.batch.schedule;

import java.time.Duration;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kafkick.batch.config.RunningJobProbe;
import com.kafkick.core.support.TimeProvider;

/**
 * <b>만료 뒤 20분에 둔다.</b> 순서가 필요해서가 아니다 — 정리는 검증이 남긴 파생 행만 걷고
 * 만료가 만드는 것을 하나도 안 읽는다. 20분은 <b>자원 분리</b>다. 만료가 밀려도 겹치지
 * 않을 폭이고, 그 경계를 {@code BatchJobRunningTooLong} 이 600초에서 지킨다.
 *
 * <p><b>도는 검증을 안 걷는 것은 이 클래스가 아니라 잡 안의 두 방어선이다.</b> 스케줄러는
 * 검증을 기다리지 않는다(기다리면 정리가 검증 뒤로 밀려 배치 창을 벗어난다). 대신
 * {@code purgeVerificationRunsStep} 이 청크마다 {@link RunningJobProbe} 로 <i>"지금 검증이
 * 도는가"</i> 를 묻고, 그 아래에 시각 창 둘이 깔린다 — 판정이 비어 있고 최근에 시작한
 * 실행은 개수와 무관하게 남기고({@code openRunGrace}), 그보다 오래된 열린 실행은
 * <b>보존 창을 우회해</b> 걷는다({@code abandoned-after-hours}).
 * <b>보존 창(개수)이 지켜 주는 것이 아니다</b> — 그렇게 읽으면 {@code ASOF_STATE_KEEP_RUNS}
 * 를 내릴 때 자기가 무엇을 푸는지 모른다.
 *
 * <p><b>{@code batch.scheduling.enabled} 하나로 멈춘다.</b> 만료와 같은 스위치를 쓴다 —
 * 끌 것이 여러 개면 하나는 반드시 빠뜨린다.
 */
@Component
@ConditionalOnProperty(name = "batch.scheduling.enabled", havingValue = "true",
        matchIfMissing = false)
public class CleanupScheduler {

    /**
     * <b>{@code @Scheduled} 와 생성자가 같은 문자열을 읽어야 한다.</b> 리터럴을 두 곳에 적어
     * 두면 한쪽만 고치는 실수를 아무것도 막지 못한다({@code @Scheduled} 는 컴파일 상수만 받는다).
     */
    static final String CRON = "${batch.schedule.cleanup-cron:0 30 4 * * *}";

    /** 만료와 같은 좌표계를 봐야 한다. 이유는 {@link ExpireScheduler#ZONE} 에 적었다. */
    static final String ZONE = "${batch.schedule.zone:UTC}";

    /**
     * 크론 최대간격을 재는 창. {@code ExpireScheduler} 와 같은 값이고 이유도 같다 —
     * 연 1회 크론(다음 1월 1일은 언제나 365일 안이다)까지 재야 SLA 가 판단을 진다.
     */
    private static final Duration SLA_CHECK_HORIZON = Duration.ofDays(400);

    private static final Logger log = LoggerFactory.getLogger(CleanupScheduler.class);

    private final JobOperator jobOperator;
    private final Job cleanupJob;
    private final TimeProvider timeProvider;
    private final CronSlot cronSlot;
    private final String cleanupCron;

    /**
     * <b>주기와 SLA 를 한자리에서 맞춘다.</b> {@code CleanupNotSucceeding} 은
     * {@code time() - 마지막성공 > SLA} 로 보는데, 크론을 늘리면 정상 상태에서도 그 간격이
     * SLA 를 넘는다 — 그러면 <b>정상인데 매일 warning</b> 이 뜨고, 그 알림이 무시되기
     * 시작하면 진짜 정리 정지도 같은 알림으로 묻힌다. 이 저장소가 없애려는 상태 그대로다.
     * {@code ExpireScheduler} 가 같은 이유로 같은 검사를 한다.
     *
     * <p>{@code JobOperator} 빈이 둘이다. 이 잡은 <b>공용(동기)</b> 빈이어야 한다 — 비동기
     * 빈이 물리면 크론의 겹침 방지가 사라지고, 정리가 자기 자신과 겹쳐 같은 대상을 두 번
     * 훑는다({@code CleanupSchedulerTest} 가 그 배선을 못 박아 둔다).
     */
    public CleanupScheduler(@Qualifier("jobOperator") JobOperator jobOperator,
            @Qualifier("cleanupJob") Job cleanupJob,
            TimeProvider timeProvider,
            @Value(CRON) String cleanupCron,
            @Value("${batch.metrics.cleanup-sla-seconds:90000}") long slaSeconds,
            @Value("${batch.metrics.run-refresh-ms:60000}") long refreshMillis) {
        if (Scheduled.CRON_DISABLED.equals(cleanupCron)) {
            // 끄는 수단은 하나여야 한다. "-" 로 끄면 트리거만 죽고 알림은 그대로 살아
            // 25시간 뒤부터 영원히 운다 — 끈 것을 아무도 알림에 말해 주지 않는다.
            throw new IllegalArgumentException(
                    "정리를 끄려면 batch.scheduling.enabled=false 를 쓰십시오. "
                            + "batch.schedule.cleanup-cron 의 \"-\" 는 트리거만 끄고 "
                            + "CleanupNotSucceeding 은 그대로 울립니다.");
        }
        this.cronSlot = new CronSlot(cleanupCron);
        this.cleanupCron = cleanupCron;
        Duration worstGap = this.cronSlot
                .maxGap(timeProvider.now(), SLA_CHECK_HORIZON)
                .map(gap -> gap.plusMillis(refreshMillis))
                .orElseThrow(() -> new IllegalArgumentException(
                        "정리 크론이 " + SLA_CHECK_HORIZON.toDays() + "일 안에 한 번도 안 돕니다. "
                                + "그런 주기로는 CleanupNotSucceeding 의 SLA(" + slaSeconds
                                + "초)를 만족할 수 없습니다 — 정리를 끄려면 "
                                + "batch.scheduling.enabled=false 를 쓰십시오. cron=" + cleanupCron));
        if (worstGap.toSeconds() >= slaSeconds) {
            throw new IllegalArgumentException(
                    "정리 지연 상한이 CleanupNotSucceeding 의 SLA 를 넘습니다. "
                            + "크론 최대간격 + run-refresh-ms = " + worstGap.toSeconds()
                            + "초 >= SLA " + slaSeconds + "초. 정상 상태에서 오탐 warning 이 "
                            + "납니다 — 크론을 촘촘히 하거나 batch.metrics.cleanup-sla-seconds 를 "
                            + "올리십시오(알림 식의 초 값도 함께 고쳐야 합니다). "
                            + "cron=" + cleanupCron);
        }
        this.jobOperator = jobOperator;
        this.cleanupJob = cleanupJob;
        this.timeProvider = timeProvider;
    }

    /**
     * <b>예외를 밖으로 던지지 않는다.</b> {@code @Scheduled} 에서 예외가 나가면 스프링이
     * 로그만 남기고 다음 주기를 잡는데, 그러면 정리가 <b>조용히 안 도는 상태</b>가 된다.
     * {@code ExpireScheduler} 가 같은 이유로 같은 모양을 쓴다.
     *
     * <p><b>{@code JobOperator.start} 는 잡이 실패로 끝나도 예외를 안 던진다</b> — 실행 결과를
     * 돌려줄 뿐이다. 그래서 상태를 직접 본다.
     *
     * <p>실행 시각을 파라미터로 싣는 것은 <b>인스턴스를 가르기 위해서</b>다. 안 실으면 두 번째
     * 실행이 {@code JobInstanceAlreadyCompleteException} 으로 죽는다. 이 값은 만료의
     * {@code asOf} 와 달리 <b>판정에 안 쓰인다</b> — 정리는 "지금 남아 있는 것" 을 본다.
     */
    @Scheduled(cron = CRON, zone = ZONE)
    public void cleanup() {
        // **슬롯에 맞춘다.** 원값을 쓰면 노드마다 값이 갈려 JOB_INST_UN 이 아무것도 거부하지
        // 못한다 — 겹침 방지가 한 JVM 안에서만 성립한다. ExpireScheduler 가 asOf 에 같은
        // 이유로 같은 모양을 쓴다. 삭제는 멱등이라 지금 손상은 없지만, 배치를 두 대로
        // 늘리는 날 같은 대상을 둘이 훑는다.
        LocalDateTime firedAt = cronSlot.atOrBefore(timeProvider.now());
        if (firedAt == null) {
            log.error("정리 크론 슬롯을 구하지 못해 이번 주기를 건너뜁니다. cron={}", cleanupCron);
            return;
        }
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("firedAt", firedAt)
                .toJobParameters();
        try {
            JobExecution execution = jobOperator.start(cleanupJob, parameters);
            BatchStatus status = execution.getStatus();
            if (status.isRunning()) {
                log.error("정리 배치가 비동기로 떴습니다. 이 잡은 동기 실행을 전제로 겹침을 "
                        + "막습니다. JobOperator 의 TaskExecutor 를 확인하십시오. "
                        + "firedAt={} 상태={}", firedAt, status);
            } else if (status.isUnsuccessful()) {
                log.error("정리 배치가 끝내지 못했습니다. firedAt={} 상태={} 원인={}",
                        firedAt, status, execution.getAllFailureExceptions());
            }
        } catch (Exception e) {
            log.error("정리 배치를 시작하지 못했습니다. firedAt={}", firedAt, e);
        }
    }
}

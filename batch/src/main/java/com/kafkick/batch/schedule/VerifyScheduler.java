// 정합성 검증을 배치 창에 띄웁니다.
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

import com.kafkick.batch.config.BatchJobRepositoryConfig;
import com.kafkick.batch.config.VerifyRunContext;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.verification.DatasetType;
import com.kafkick.core.verification.ScopeType;

/**
 * <b>만료 04:10 · 정리 04:30 다음의 05:00 이다.</b> 순서가 아니라 <b>상호 배제</b>가 이유다 —
 * {@code ExpireScheduler} 는 검증이 도는 동안 슬롯을 건너뛰고, 그 건너뛰기가 만료 SLA 예산을
 * 먹는다. 두 잡이 시간으로 갈려 있으면 건너뛸 일이 없어 {@code max-expire-skips} 를 0 으로
 * 내릴 수 있고, 그래야 만료 SLA 가 50시간에서 25시간으로 조여진다(CY-470 이 함께 한 일).
 *
 * <p><b>50분이 어디서 왔나.</b> 만료가 04:10 에 시작해 05:00 전에 끝나야 겹치지 않는다.
 * {@code BatchJobRunningTooLong} 이 만료를 600초에서 잡으므로 정상 상태의 상한이 10분이고,
 * 50분은 그 다섯 배다. 검증 자신은 300만 행에서 <b>472초</b>가 걸렸으므로(CY-470 실측)
 * 05:08 에 끝나 다음 날 04:10 과 21시간 떨어져 있다.
 *
 * <h2>이 크론이 성립하는 전제 — 발급이 멈춘 창이어야 한다</h2>
 *
 * <p><b>{@code startRunStep} 의 {@code rejectIssuancesUpdatedAfterAsOf} 가 그것을 강제한다.</b>
 * {@code asOf} 보다 뒤에 갱신된 발급건이 하나라도 있으면 그 실행은
 * {@code DATASET_MUTATED_DURING_RUN} 으로 죽는다 — 정합성 검증은 <b>쓰기가 멈춘 스냅샷</b>
 * 위에서만 뜻이 있기 때문이고, 만료가 찍은 {@code updated_at} 은 지워지지 않으므로
 * <b>그 {@code asOf} 는 영구히 못 쓴다.</b>
 *
 * <p>그래서 이 슬롯에 발급 트래픽이 있으면 검증은 <b>매일 실패한다.</b> 그것은 버그가 아니라
 * 전제가 깨졌다는 보고다 — {@code VerifyNotSucceeding} 이 그 사실을 진다. 슬롯을 옮겨야
 * 한다면 {@code batch.schedule.verify-cron} 을 바꾸되 <b>만료·정리와 겹치지 않는 자리</b>여야
 * 하고, 겹치게 두면 {@code max-expire-skips=0} 이 검증을 뚫고 지나간다.
 *
 * <p><b>{@code batch.scheduling.enabled} 하나로 멈춘다.</b> 만료·정리와 같은 스위치다 —
 * 끌 것이 여러 개면 하나는 반드시 빠뜨린다. 끄면 <b>온디맨드 API 는 그대로 산다</b>
 * ({@code VerifyTriggerController} 는 이 플래그를 안 본다).
 */
@Component
@ConditionalOnProperty(name = "batch.scheduling.enabled", havingValue = "true",
        matchIfMissing = false)
public class VerifyScheduler {

    /**
     * <b>{@code @Scheduled} 와 생성자가 같은 문자열을 읽어야 한다.</b> 갈리면 트리거는 A 크론으로
     * 발화하는데 {@code CronSlot} 은 B 슬롯을 계산해 {@code asOf} 가 발화 시각과 어긋난다.
     * 근거는 {@link ExpireScheduler#CRON} 에 적었다.
     */
    static final String CRON = "${batch.schedule.verify-cron:0 0 5 * * *}";

    /** 만료·정리와 같은 좌표계를 봐야 한다. 이유는 {@link ExpireScheduler#ZONE} 에 적었다. */
    static final String ZONE = "${batch.schedule.zone:UTC}";

    /**
     * <b>게이트가 보는 조합이다.</b> {@code cy_verify_last_success_seconds} 도 같은 상수로
     * 게이지를 내므로 <b>둘이 갈릴 자리가 없다</b> — 크론이 다른 조합을 돌면 그 게이지는
     * 영원히 비어 있고 {@code VerifyNeverSucceeded} 가 배포 첫날부터 운다.
     *
     * <p>{@code enum} 으로 되받는 것은 <b>오타를 컴파일에서 잡기 위해서</b>다. 그대로
     * 문자열을 실으면 {@code VerifyRunContext} 쪽 값이 도메인에 없는 이름이 되어도
     * 잡이 뜬 뒤 {@code startRunStep} 에서야 죽는다.
     */
    static final DatasetType DATASET = DatasetType.valueOf(VerifyRunContext.SLA_DATASET);

    /** 같은 이유로 {@code FULL} 이다. 증분은 스케줄러가 없고 온디맨드 경로도 아직 거절한다. */
    static final ScopeType SCOPE = ScopeType.valueOf(VerifyRunContext.SLA_SCOPE);

    /**
     * <b>슬롯마다 {@code asOf} 가 달라 1 로 충분하다.</b> 시드가 점유한 것은
     * <i>시드 자신의</i> {@code asOf} 위의 1·2 이지 모든 {@code asOf} 가 아니다
     * ({@code VerifyJobConfig#rejectExistingRun}). 같은 슬롯을 손으로 다시 돌려야 하면
     * 그때 2 부터 쓴다 — 크론이 번호를 올려 가며 재시도하지 <b>않는</b> 것은,
     * 실패의 원인이 대개 "그 {@code asOf} 로는 이제 못 잰다" 여서 재시도가 뜻이 없어서다.
     */
    private static final long ATTEMPT = 1L;

    private static final Logger log = LoggerFactory.getLogger(VerifyScheduler.class);

    private final JobOperator jobOperator;
    private final Job verifyJob;
    private final TimeProvider timeProvider;
    private final CronSlot cronSlot;
    private final String verifyCron;

    /**
     * <b>주기와 SLA 를 한자리에서 맞춘다.</b> {@code VerifyNotSucceeding} 은
     * {@code time() - 마지막성공 > SLA} 로 보는데, 크론을 늘리면 정상 상태에서도 그 간격이
     * SLA 를 넘어 <b>사고 없이 매일 운다.</b> 그 알림이 무시되기 시작하면 진짜 검증 정지도
     * 같은 알림으로 묻힌다 — {@code ExpireScheduler}·{@code CleanupScheduler} 가 같은 이유로
     * 같은 검사를 한다.
     *
     * <p><b>{@code JobOperator} 빈이 둘이다. 이 잡은 공용(동기) 빈이어야 한다.</b> 비동기 빈은
     * {@code VerifyTriggerController} 의 것으로, HTTP 요청에 202 를 돌려주려고 존재한다.
     * 크론에 그것이 물리면 {@code start} 가 즉시 {@code STARTED} 를 돌려주고 아래
     * {@code isUnsuccessful()} 이 아무것도 못 보므로, <b>실패가 로그 한 줄 없이 사라진다.</b>
     * 겹침 방지도 함께 사라진다 — 8분짜리 잡이 자기 자신과 겹치지 않는 것은 스프링의
     * 크론 트리거가 직전 실행을 기다리기 때문이다.
     */
    public VerifyScheduler(
            @Qualifier(BatchJobRepositoryConfig.SHARED_OPERATOR) JobOperator jobOperator,
            @Qualifier("verifyJob") Job verifyJob,
            TimeProvider timeProvider,
            @Value(CRON) String verifyCron,
            @Value("${batch.metrics.verify-sla-seconds:90000}") long slaSeconds,
            @Value("${batch.metrics.run-refresh-ms:60000}") long refreshMillis) {
        if (Scheduled.CRON_DISABLED.equals(verifyCron)) {
            // 끄는 수단은 하나여야 한다. "-" 로 끄면 트리거만 죽고 알림은 그대로 살아
            // SLA 를 넘긴 뒤부터 영원히 운다 — 끈 것을 아무도 알림에 말해 주지 않는다.
            throw new IllegalArgumentException(
                    "검증을 끄려면 batch.scheduling.enabled=false 를 쓰십시오. "
                            + "batch.schedule.verify-cron 의 \"-\" 는 트리거만 끄고 "
                            + "VerifyNotSucceeding 은 그대로 울립니다.");
        }
        this.cronSlot = new CronSlot(verifyCron);
        this.verifyCron = verifyCron;

        // 건너뛰기 항이 0 이다 — 검증은 슬롯을 건너뛰지 않는다. 앞 실행이 아직 돌면
        // 스프링의 크론 트리거가 애초에 다음 발화를 안 잡기 때문이다.
        // 소요 항은 SlaBudget 이 진다(게이지가 END_TIME 이라 도는 동안 나이가 자란다).
        Duration worstAge = SlaBudget.worstAge(this.cronSlot, timeProvider.now(), 0,
                        refreshMillis, SlaBudget.VERIFY_RUNNING_TOO_LONG_SECONDS)
                .orElseThrow(() -> new IllegalArgumentException(
                        "검증 크론이 " + SlaBudget.CHECK_HORIZON.toDays()
                                + "일 안에 한 번도 안 돕니다. "
                                + "그런 주기로는 VerifyNotSucceeding 의 SLA(" + slaSeconds
                                + "초)를 만족할 수 없습니다 — 검증을 끄려면 "
                                + "batch.scheduling.enabled=false 를 쓰십시오. cron=" + verifyCron));
        if (worstAge.toSeconds() >= slaSeconds) {
            throw new IllegalArgumentException(
                    "검증 지연 상한이 VerifyNotSucceeding 의 SLA 를 넘습니다. "
                            + "크론 최대간격 + run-refresh-ms + VerifyRunningTooLong("
                            + SlaBudget.VERIFY_RUNNING_TOO_LONG_SECONDS + "초) = "
                            + worstAge.toSeconds()
                            + "초 >= SLA " + slaSeconds + "초. 정상 상태에서 오탐 critical 이 "
                            + "납니다 — 크론을 촘촘히 하거나 batch.metrics.verify-sla-seconds 를 "
                            + "올리십시오(batch-alerts.yml 의 VerifyNotSucceeding 식의 초 값도 "
                            + "함께 고쳐야 합니다). cron=" + verifyCron);
        }
        this.jobOperator = jobOperator;
        this.verifyJob = verifyJob;
        this.timeProvider = timeProvider;
    }

    /**
     * <b>예외를 밖으로 던지지 않는다.</b> {@code @Scheduled} 에서 예외가 나가면 스프링이 로그만
     * 남기고 다음 주기를 잡아, 검증이 <b>조용히 안 도는 상태</b>가 된다. 만료·정리가 같은 이유로
     * 같은 모양을 쓴다.
     *
     * <p><b>{@code JobOperator.start} 는 잡이 실패로 끝나도 예외를 안 던진다</b> — 실행 결과를
     * 돌려줄 뿐이라 상태를 직접 본다.
     *
     * <p><b>{@code asOf} 를 슬롯에 맞춘다.</b> 원값({@code now()})을 쓰면 노드마다 값이 갈려
     * {@code JOB_INST_UN} 이 아무것도 거부하지 못하고, 배치를 두 대로 늘리는 날 같은 데이터를
     * 둘이 동시에 판정한다 — 그리고 그 둘은 서로를
     * {@code DATASET_MUTATED_DURING_RUN} 으로 죽인다. 만료가 같은 이유로 같은 모양을 쓴다.
     */
    @Scheduled(cron = CRON, zone = ZONE)
    public void verify() {
        LocalDateTime firedAt = cronSlot.atOrBefore(timeProvider.now());
        if (firedAt == null) {
            log.error("검증 크론 슬롯을 구하지 못해 이번 주기를 건너뜁니다. cron={}", verifyCron);
            return;
        }
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", firedAt)
                .addString("scope", SCOPE.name())
                .addString("dataset", DATASET.name())
                .addLong("attempt", ATTEMPT)
                .toJobParameters();
        try {
            JobExecution execution = jobOperator.start(verifyJob, parameters);
            BatchStatus status = execution.getStatus();
            if (status.isRunning()) {
                log.error("검증 배치가 비동기로 떴습니다. 이 잡은 동기 실행을 전제로 겹침을 "
                        + "막습니다. JobOperator 의 TaskExecutor 를 확인하십시오. "
                        + "asOf={} 상태={}", firedAt, status);
            } else if (status.isUnsuccessful()) {
                // 원인 대부분은 "그 asOf 로는 이제 못 잰다" 다 — 이 슬롯에 발급이 있었다는
                // 뜻이고, 클래스 javadoc 의 전제가 깨진 것이다. 재시도가 뜻이 없으므로
                // 여기서 attempt 를 올려 다시 부르지 않는다.
                log.error("검증 배치가 판정을 내지 못했습니다. 이 슬롯에 발급이 있었다면 "
                        + "이 asOf 로는 다시 검증할 수 없습니다 — 발급이 멈춘 창으로 "
                        + "batch.schedule.verify-cron 을 옮겨야 합니다. asOf={} 상태={} 원인={}",
                        firedAt, status, execution.getAllFailureExceptions());
            }
        } catch (Exception e) {
            log.error("검증 배치를 시작하지 못했습니다. asOf={}", firedAt, e);
        }
    }
}

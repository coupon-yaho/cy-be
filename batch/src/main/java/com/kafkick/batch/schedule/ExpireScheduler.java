// 만료 잡을 주기로 띄웁니다. 부하 중에는 이 빈이 아예 만들어지지 않습니다.
package com.kafkick.batch.schedule;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kafkick.core.support.TimeProvider;

/**
 * <b>{@code batch.scheduling.enabled} 하나로 멈춘다.</b> 조건을 이 빈에 걸어 두면
 * 플래그가 꺼졌을 때 빈이 만들어지지 않는다 — 메서드 안에서 검사하고 빠져나오는 방식이면
 * 검사를 빠뜨린 스케줄러가 하나 생기는 순간 조용히 돈다.
 *
 * <p><b>속성이 아예 없으면 안 돈다({@code matchIfMissing = false}).</b> 두 사고의 무게가
 * 다르기 때문이다 — 켜진 채 검증 셋을 보면 되돌릴 수 없고, 꺼진 채 운영을 보면 나중에 돌려
 * 따라잡을 수 있기 때문이다.
 *
 * <p><b>예전에 여기 "{@code BatchJobNotRunning} 알림이 잡아 준다" 고 적었다. 아직 아니다.</b>
 * 규칙 파일은 {@code infra/prometheus/rules/batch-alerts.yml} 에 있지만 <b>그것을 읽는
 * Prometheus 가 아직 배선돼 있지 않다</b>({@code infra/prometheus/prometheus.yml} 이 그 사실을
 * 적어 두었다). compose 가 들어오는 티켓 전까지 이 기본값의 반대편 사고 — 스케줄러가 꺼진 채
 * 뜨는 것 — 는 <b>감지 수단이 없다.</b>
 *
 * <p><b>{@code VerifyJobConfig} 의 같은 키는 기본값이 반대({@code :true})이고 그것이 맞다.</b>
 * 거기서는 이 플래그를 <i>"스케줄러가 도는 중인가"</i> 로 읽어 검증을 <b>거부</b>하는 데 쓴다.
 * 모를 때 도는 것으로 보고 거부하는 쪽이 안전하다. 두 기본값을 일관성 때문에 맞추면
 * <b>한쪽은 반드시 위험한 방향이 된다.</b>
 *
 * <p><b>끌 것이 하나여야 하는 이유가 이 잡에 있다.</b> 만료는 재고를 쓴다.
 * 부하 측정 중에 돌면 같은 DB 를 때려 측정값이 흔들리고, 검증이 도는 중에 돌면
 * {@code dataset_fingerprint} 재료인 {@code sum(active_count)} 가 바뀌어
 * <b>판정이 데이터 변동으로 죽는다.</b>
 *
 * <p><b>{@code asOf} 를 분 단위로 자른다.</b> Spring Batch 는 같은 파라미터로 완료된 실행을
 * 다시 돌리지 않으므로 이 값이 곧 실행의 신원이자 중복 방지 장치가 된다. 초를 남기면
 * 매 실행이 서로 다른 신원을 갖게 되어 <b>그 방지가 아예 안 걸린다</b> — 배포 직후처럼
 * 같은 주기에 두 번 뜨는 상황에서 둘 다 돈다.
 */
@Component
@ConditionalOnProperty(name = "batch.scheduling.enabled", havingValue = "true",
        matchIfMissing = false)
public class ExpireScheduler {

    private static final Logger log = LoggerFactory.getLogger(ExpireScheduler.class);

    private final JobOperator jobOperator;
    private final Job expireJob;
    private final TimeProvider timeProvider;

    // Job 빈이 expireJob·verifyJob 둘이다. 지금은 파라미터 이름으로 갈리지만(부트 그래들
    // 플러그인이 -parameters 를 붙인다) 그 기본값에 기대는 대신 명시한다 — 셋째 Job 이
    // 생기거나 컴파일 옵션이 바뀌면 조용히 다른 잡이 주입되고, 그때 5분마다 도는 것이
    // 만료가 아니게 된다.
    public ExpireScheduler(JobOperator jobOperator,
            @Qualifier("expireJob") Job expireJob,
            TimeProvider timeProvider) {
        this.jobOperator = jobOperator;
        this.expireJob = expireJob;
        this.timeProvider = timeProvider;
    }

    /**
     * <b>예외를 밖으로 던지지 않는다.</b> {@code @Scheduled} 에서 예외가 나가면 스프링이
     * 로그만 남기고 다음 주기를 계속 잡는다 — 재고를 쓰는 유일한 잡이 <b>조용히 안 도는 상태</b>가
     * 되고, 아무도 그것을 모른다. 다음 주기가 또 오므로 한 번 실패가 곧 장애도 아니다.
     *
     * <p><b>시작하지 못한 것과 돌다가 실패한 것은 다른 사건이다.</b>
     * {@code JobOperator.start} 는 잡이 {@code FAILED} 로 끝나도 <b>예외를 던지지 않는다</b> —
     * 실행 결과를 돌려줄 뿐이다. 그래서 상태를 직접 봐야 한다. 안 보면 이력 짝 불일치나
     * 재고 행 누락으로 잡이 멈춰도 <b>로그가 한 줄도 남지 않는다.</b>
     *
     * <p><b>네 갈래를 레벨로 가른다.</b> 뭉쳐 두면 알림이 붙는 순간 하루 288번 중 대부분이
     * 소음이 되고, 진짜 사건이 그 안에 묻힌다.
     *
     * <table border="1">
     *   <caption>상황별 레벨</caption>
     *   <tr><td>정상 종료</td><td>남기지 않는다</td><td>대부분의 실행이 0건이다</td></tr>
     *   <tr><td>이미 끝난 {@code asOf}</td><td>INFO</td>
     *       <td>중복 방지가 제 일을 한 것이다. 사건이 아니다</td></tr>
     *   <tr><td>비종단 상태로 반환</td><td>ERROR</td>
     *       <td>동기 전제가 깨졌다. 아래 설명</td></tr>
     *   <tr><td>앞 실행이 아직 돎</td><td>WARN</td>
     *       <td>지금 구조에서는 도달하지 않는다. 아래 설명</td></tr>
     *   <tr><td>시작 실패 · 잡 실패</td><td>ERROR</td><td>사람이 봐야 한다</td></tr>
     * </table>
     *
     * <p><b>겹침을 막는 것은 이 catch 절이 아니라 크론 트리거다.</b> 스프링의
     * {@code ReschedulingRunnable} 이 다음 실행을 <b>직전 실행이 끝난 뒤</b> 잡으므로
     * {@code expire()} 는 자기 자신과 겹치지 않는다. {@code JobExecutionAlreadyRunningException}
     * 은 <b>같은 JobInstance</b> 에만 나는데 우리는 주기마다 {@code asOf} 가 달라
     * 매번 새 인스턴스다 — 그래서 저 WARN 갈래는 <b>지금 구조에서 도달하지 않는다.</b>
     * 남겨 두는 것은 다중 인스턴스로 늘어나는 날을 위해서다(그때는 같은 {@code asOf} 로
     * 두 서버가 부딪힌다).
     *
     * <p><b>스케줄러 풀은 batch 의 모든 {@code @Scheduled} 가 공유한다.</b> CY-254 가
 * {@code spring.task.scheduling.pool.size} 를 1 에서 4 로 올린다. 그것이 이 잡을 자기 자신과
 * 겹치게 만들지는 않는다 — 위 문단대로 크론 트리거가 직전 실행을 기다리기 때문이고,
 * 풀 크기와 무관하다. <b>바뀌는 것은 다른 스케줄러와 나란히 도는 것</b>이다.
 *
 * <p>그래서 재고를 쓰는 배치가 늘어나는 날 이 잡과 <b>동시에</b> 돈다. 설계상 재고를 쓰는 것은
 * 지금 이 잡뿐이고, 그 전제가 깨지면 락 순서 계약(`issuances` → `issuance_histories` →
 * {@code coupon_stocks})을 그쪽도 지켜야 한다 — {@code ExpirationRepository} 에 적어 뒀다.
 *
 * <p><b>그 보호는 동기 실행을 전제한다.</b> {@code JobOperator} 에 비동기
     * {@code TaskExecutor} 가 물리면 {@code start} 가 즉시 {@code STARTED} 를 돌려주고
     * 크론의 겹침 방지가 통째로 사라진다 — 그런데 {@code isUnsuccessful()} 은 그 상태를
     * 실패로 보지 않아 <b>아무 로그도 안 남는다.</b> 전제가 깨진 것을 그 자리에서 알리려고
     * 비종단 상태를 따로 잡는다.
     *
     * <p>알림 경로가 붙는 티켓이 이 자리를 쓴다 — 멘토가 말한 <i>"사람이 어떻게 알 수 있느냐"</i>
     * 가 여기고, 그때 필요한 것은 <b>레벨이 이미 갈려 있는 로그</b>다.
     */
    @Scheduled(cron = "${batch.schedule.expire-cron:0 */5 * * * *}")
    public void expire() {
        LocalDateTime asOf = timeProvider.now().truncatedTo(ChronoUnit.MINUTES);
        try {
            JobExecution execution = jobOperator.start(expireJob, new JobParametersBuilder()
                    .addLocalDateTime("asOf", asOf)
                    .toJobParameters());
            BatchStatus status = execution.getStatus();
            if (status.isRunning()) {
                // 동기 전제가 깨졌다. 크론의 겹침 방지가 사라진 상태라, 다음 주기가
                // 앞 실행과 겹쳐 서로를 잠근다.
                log.error("만료 배치가 비동기로 떴습니다. 이 잡은 동기 실행을 전제로 겹침을 "
                        + "막습니다. JobOperator 의 TaskExecutor 를 확인하십시오. "
                        + "asOf={} 상태={}", asOf, status);
            } else if (status.isUnsuccessful()) {
                // 원인을 여기 함께 남긴다. 잡 쪽 로그와 떨어져 있으면 알림만 보고는
                // 무엇 때문에 멈췄는지 알 수 없다.
                log.error("만료 배치가 끝내지 못했습니다. asOf={} 상태={} 원인={}",
                        asOf, status, execution.getAllFailureExceptions());
            }
        } catch (JobExecutionAlreadyRunningException e) {
            // 지금 구조에서는 도달하지 않는다(클래스 주석 참조). 다중 인스턴스로 늘어나면
            // 같은 asOf 로 두 서버가 부딪히면서 이 자리가 산다.
            log.warn("앞 실행이 아직 돌고 있어 이번 주기를 건너뜁니다. asOf={}", asOf);
        } catch (JobInstanceAlreadyCompleteException e) {
            log.info("이미 끝난 asOf 라 건너뜁니다. asOf={}", asOf);
        } catch (Exception e) {
            log.error("만료 배치를 시작하지 못했습니다. asOf={}", asOf, e);
        }
    }
}

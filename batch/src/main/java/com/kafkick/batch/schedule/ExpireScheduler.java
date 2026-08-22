// 만료 잡을 주기로 띄웁니다. 부하 중에는 이 빈이 아예 만들어지지 않습니다.
package com.kafkick.batch.schedule;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
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
 * <p><b>{@code asOf} 를 크론 슬롯에서 뽑는다.</b> Spring Batch 는 같은 파라미터로 완료된
 * 실행을 다시 돌리지 않으므로 이 값이 곧 실행의 신원이자 중복 방지 장치다
 * (그 방지가 성립하려면 {@code BATCH_JOB_INSTANCE} 가 DB 에 남아야 한다 —
 * {@code BatchJobRepositoryConfig}).
 *
 * <p>예전에는 {@code now().truncatedTo(MINUTES)} 였다. <b>실행된 시각</b>을 자른 것이라
 * 두 가지가 어긋났다 — 노드마다 실행이 밀리는 정도가 달라 {@code asOf} 가 갈리면 서로 다른
 * JobInstance 가 되어 <b>중복 방지가 아예 발동하지 않고</b>, 주기를 1분 미만으로 줄이면
 * 두 발화가 같은 분에 들어가 <b>절반이 조용히 건너뛰어진다.</b> 슬롯에서 뽑으면 늦게 떠도
 * 같은 슬롯이면 같은 값이고, 주기가 짧아지면 슬롯도 촘촘해진다. {@link CronSlot} 참조.
 */
@Component
@ConditionalOnProperty(name = "batch.scheduling.enabled", havingValue = "true",
        matchIfMissing = false)
public class ExpireScheduler {

    private static final Logger log = LoggerFactory.getLogger(ExpireScheduler.class);

    /**
     * <b>{@code @Scheduled} 와 생성자가 같은 문자열을 읽어야 한다.</b> 둘이 갈리면 트리거는
     * A 크론으로 발화하는데 {@code CronSlot} 은 B 크론 슬롯을 계산해, 노드마다 다른
     * {@code asOf} 가 나오고 중복 방지가 아예 발동하지 않는다. 리터럴을 두 곳에 적어 두면
     * 한쪽만 고치는 실수를 아무것도 안 막으므로 상수로 묶는다({@code @Scheduled} 는 컴파일
     * 상수만 받는다).
     */
    static final String CRON = "${batch.schedule.expire-cron:0 */5 * * * *}";

    /**
     * <b>발화와 슬롯이 같은 좌표계를 봐야 한다.</b> {@code @Scheduled} 는 {@code zone} 을 안 주면
     * <b>JVM 기본 타임존</b>으로 크론을 풀고, {@code CronSlot} 은 {@code TimeProvider}
     * ({@code Clock.systemUTC})가 준 값으로 푼다. 개발 기기가 KST 면 그 둘이 9시간 어긋난다.
     *
     * <p>지금 기본값 {@code 0 *}{@code /5 * * * *} 에서만 우연히 안 드러난다 — 실존하는 오프셋이
     * 전부 15분 배수라 UTC 로 옮겨도 5분 슬롯 위에 그대로 떨어지기 때문이다.
     * <b>시(hour) 필드가 들어가는 순간 깨진다</b>({@code coupon-create-cron} 이 이미 그 모양이다).
     * 그때 {@code asOf} 가 발화 시각보다 몇 시간 과거가 되고, {@code asOf <= now} 는 계속
     * 성립하므로 {@code EXPIRE_ASOF_IN_FUTURE} 가드도 안 울린다 — <b>조용히 그만큼 밀린다.</b>
     */
    static final String ZONE = "${batch.schedule.zone:UTC}";

    private final JobOperator jobOperator;
    private final Job expireJob;
    private final TimeProvider timeProvider;
    private final CronSlot cronSlot;
    private final String expireCron;

    // Job 빈이 expireJob·verifyJob 둘이다. 지금은 파라미터 이름으로 갈리지만(부트 그래들
    // 플러그인이 -parameters 를 붙인다) 그 기본값에 기대는 대신 명시한다 — 셋째 Job 이
    // 생기거나 컴파일 옵션이 바뀌면 조용히 다른 잡이 주입되고, 그때 5분마다 도는 것이
    // 만료가 아니게 된다.
    public ExpireScheduler(JobOperator jobOperator,
            @Qualifier("expireJob") Job expireJob,
            TimeProvider timeProvider,
            @Value(CRON) String expireCron) {
        this.jobOperator = jobOperator;
        this.expireJob = expireJob;
        this.timeProvider = timeProvider;
        if (Scheduled.CRON_DISABLED.equals(expireCron)) {
            // @Scheduled 는 "-" 를 "이 트리거를 끈다" 로 받지만 CronSlot 은 asOf 를 못 만든다.
            // 그대로 두면 CronExpression.parse 가 던져 batch 앱 전체가 기동에 실패하고,
            // verifyJob 까지 같이 못 뜬다. 무엇을 써야 하는지 여기서 말한다.
            throw new IllegalArgumentException(
                    "만료를 끄려면 batch.scheduling.enabled=false 를 쓰십시오. "
                            + "batch.schedule.expire-cron 의 \"-\" 는 트리거만 끄고 "
                            + "asOf 를 만들 근거가 사라집니다.");
        }
        this.cronSlot = new CronSlot(expireCron);
        this.expireCron = expireCron;
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
     * <p><b>스케줄러 풀은 batch 의 모든 {@code @Scheduled} 가 공유한다.</b> CY-359 가
     * {@code spring.task.scheduling.pool.size} 를 <b>2</b> 로 올렸다 — {@code @Scheduled} 가
     * 둘이라서다(이 잡과 검증 판정 되읽기). 근거는 {@code application.yml.example} 의 그 키에
     * 적혀 있다. 그것이 이 잡을 자기 자신과
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
    @Scheduled(cron = CRON, zone = ZONE)
    public void expire() {
        LocalDateTime now = timeProvider.now();
        LocalDateTime asOf = cronSlot.atOrBefore(now);
        if (asOf == null) {
            // 슬롯을 못 구했다. 여기서 대체값을 만들면 그 값이 조용히 실행의 신원이 되고,
            // 노드마다 다른 값이 나와 중복 방지가 발동하지 않는다. 도는 것보다 안 도는 것이 낫다.
            log.error("크론 슬롯을 구하지 못해 이번 주기를 건너뜁니다. cron={} now={}",
                    expireCron, now);
            return;
        }
        JobParameters parameters = new JobParametersBuilder()
                .addLocalDateTime("asOf", asOf)
                .toJobParameters();
        try {
            JobExecution execution = jobOperator.start(expireJob, parameters);
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
        } catch (IllegalStateException e) {
            // JdbcJobInstanceDao.createJobInstance 의 Assert.state("JobInstance must not already
            // exist"). 인스턴스 생성이 READ COMMITTED 라 진 쪽의 SELECT 가 안 막히고, 상대가
            // 이미 커밋했으면 1062 대신 여기로 온다 — 노드가 둘이면 프로세스 간 지연이 커서
            // 오히려 이쪽이 흔하다.
            //
            // 다만 IllegalStateException 은 이 잡 밖에서도 온다(커넥션이 안 붙는 경우 등).
            // 타입만 보고 INFO 로 내리면 진짜 실패를 삼킨다 — 실제로 그렇게 만들었다가
            // "시작하지 못하면 ERROR" 테스트가 잡았다. 인스턴스가 정말 생겼는지 물어서 가른다.
            //
            // 조회 자체도 감싼다. 원래 실패가 DB 문제면 이 조회도 같이 죽는데, 그러면
            // 아래 catch(Exception) 이 잡지 못하고 예외가 expire() 밖으로 나간다 —
            // 이 메서드가 절대 하지 않기로 한 일이다(클래스 주석).
            if (startedByAnotherNode(parameters, e)) {
                log.info("다른 노드가 같은 asOf 를 이미 시작했습니다. asOf={}", asOf);
            } else {
                log.error("만료 배치를 시작하지 못했습니다. asOf={}", asOf, e);
            }
        } catch (DuplicateKeyException e) {
            // 다른 노드가 같은 asOf 로 먼저 JOB_INST_UN 을 잡았다. 중복 방지가 제 일을 한 것이라
            // 사건이 아니다 — ERROR 로 내보내면 배치를 두 대로 늘리는 순간 하루 288번 울린다.
            // (인스턴스 생성 격리를 READ COMMITTED 로 내려 둔 덕에 오류가 1062 로 좁혀진다.
            //  SERIALIZABLE 이면 데드락 1213 이라 이 타입으로 안 온다 — BatchJobRepositoryConfig 참조)
            log.info("다른 노드가 같은 asOf 를 이미 시작했습니다. asOf={}", asOf);
        } catch (Exception e) {
            log.error("만료 배치를 시작하지 못했습니다. asOf={}", asOf, e);
        }
    }

    /**
     * 이 {@code asOf} 의 인스턴스가 이미 있는가 — 즉 <b>중복 방지가 일한 것인가.</b>
     *
     * <p>조회가 던지면 <b>모른다</b>로 본다. 원래 실패가 커넥션 문제면 이 조회도 같이 죽는데,
     * 그때 예외를 그대로 올리면 {@code expire()} 밖으로 나간다 — 이 메서드가 절대 하지 않기로
     * 한 일이다. 모를 때는 사건 쪽(ERROR)으로 기운다. 중복을 ERROR 로 한 번 내는 것보다
     * 진짜 실패를 INFO 로 삼키는 것이 나쁘다.
     */
    private boolean startedByAnotherNode(JobParameters parameters, RuntimeException cause) {
        try {
            return jobOperator.getJobInstance(expireJob.getName(), parameters) != null;
        } catch (RuntimeException lookupFailed) {
            // 원인 둘을 한 줄에 남긴다. 따로 두면 어느 쪽이 먼저인지 로그에서 안 보인다.
            log.warn("실패 원인을 가르지 못했습니다(인스턴스 조회도 실패). 원래 원인={} 조회 실패={}",
                    cause.toString(), lookupFailed.toString());
            return false;
        }
    }

}

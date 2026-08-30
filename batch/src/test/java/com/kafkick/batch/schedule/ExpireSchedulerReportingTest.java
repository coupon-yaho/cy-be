// 스케줄러가 상황을 어떤 레벨로 남기는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.util.Set;
import org.springframework.batch.core.repository.JobRepository;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.batch.config.RunningJobProbe;

/**
 * <b>5분마다 도는 잡이라 로그가 곧 알림이다.</b> 네 갈래를 한 레벨로 뭉쳐 두면 알림이 붙는
 * 순간 하루 288번 중 대부분이 소음이 되고, 진짜 사건이 그 안에 묻힌다.
 *
 * <p><b>가장 조용한 실패가 잡이 {@code FAILED} 로 끝나는 경우다.</b>
 * {@code JobOperator.start} 는 그때 <b>예외를 던지지 않는다</b> — 실행 결과를 돌려줄 뿐이다.
 * 상태를 안 보면 이력 짝 불일치나 재고 행 누락으로 잡이 멈춰도 로그가 한 줄도 안 남는다.
 * 그 사실은 {@code ExpireJobRestartTest} 가 실제 잡으로 확인한다 — 거기서 첫 실행이
 * {@code FAILED} 인데 예외 없이 반환된다.
 *
 * <p><b>여기서는 스케줄러만 떼어 본다.</b> {@code JobOperator} 를 갈아 끼워 네 상황을 직접
 * 만든다. 컨테이너를 띄우면 "앞 실행이 아직 돈다" 같은 상황은 재현 자체가 어렵고,
 * 재현하더라도 무엇을 재는지가 흐려진다.
 */
class ExpireSchedulerReportingTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 1, 15, 9, 3, 27);

    /**
     * <b>운영 기본값과 같은 일 1회 크론.</b> 슬롯 판정이 이 표현식에 달려 있다.
     *
     * <p><b>한때 5분 크론(<code>0 *&#47;5 * * * *</code>)이었다.</b> CY-397 이 만료를 배치 창으로
     * 옮긴 뒤에도 여기만 남아 있었고, CY-470 이 SLA 부등식에 잡 소요 항을 넣으면서
     * <b>그 조합이 실제로 성립하지 않는다는 것</b>이 드러났다 — 5분 크론에 건너뛰기 1,
     * SLA 900 이면 만료가 {@code BatchJobRunningTooLong} 임계(600초)만큼만 돌아도 나이가
     * 1,260초까지 가서 <b>정상 상태에서 critical</b> 이다. 기동 가드가 그것을 거절한다.
     */
    private static final String EXPIRE_CRON = "0 10 4 * * *";

    /**
     * 운영 기본값과 같다. 이 셋은 함께여야 부등식이 성립한다 —
     * {@code (0+1) × 86,400 + 60 + 600 = 87,060 < 90,000}.
     */
    private static final int MAX_SKIPS = 0;

    private static final long SLA_SECONDS = 90_000L;

    /** {@code NOW} 가 속한 크론 슬롯. 일 1회라 그날 04:10 이다. */
    private static final LocalDateTime EXPECTED_SLOT =
            NOW.withHour(4).withMinute(10).withSecond(0);

    /**
     * <b>이름만 있는 잡.</b> 예전에는 여기에 {@code null} 을 넘겼는데, 스케줄러가 실패를
     * 가르려고 {@code expireJob.getName()} 을 쓰기 시작하면서 NPE 가 났다 — 필수 협력자에
     * {@code null} 을 주면 그 협력자를 쓰는 코드가 생기는 날 엉뚱한 곳에서 터진다.
     */
    private static final Job EXPIRE_JOB = new Job() {
        @Override
        public String getName() {
            return "expireJob";
        }

        @Override
        public void execute(JobExecution execution) {
            throw new UnsupportedOperationException("이 테스트는 JobOperator 를 대신 세운다");
        }
    };

    private ListAppender<ILoggingEvent> logs;
    private ch.qos.logback.classic.Logger schedulerLog;
    private Level originalLevel;

    @BeforeEach
    void setUp() {
        logs = new ListAppender<>();
        logs.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logs.start();
        schedulerLog = (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger(ExpireScheduler.class);
        // 레벨을 안 정하면 상위 로거 설정을 따라간다. application.yml 이나 루트가 WARN 으로
        // 올라가는 순간 INFO 를 기대하는 단언이 깨지는데, 실패 메시지에는 "로그가 비었다" 만
        // 남고 원인이 로깅 설정이라는 것은 안 나온다.
        originalLevel = schedulerLog.getLevel();
        schedulerLog.setLevel(Level.TRACE);
        schedulerLog.addAppender(logs);
    }

    @AfterEach
    void tearDown() {
        schedulerLog.detachAppender(logs);
        schedulerLog.setLevel(originalLevel);
        logs.stop();
    }

    /**
     * <b>대부분의 실행이 0건이다.</b> 그때마다 무언가 남기면 진짜 사건이 안 보인다.
     */
    @Test
    @DisplayName("정상으로 끝나면 아무것도 남기지 않는다")
    void staysQuietOnSuccess() {
        scheduler(operatorReturning(execution(BatchStatus.COMPLETED, null))).expire();

        assertThat(logs.list).isEmpty();
    }

    /**
     * <b>이 갈래가 없으면 잡 실패가 조용히 지나간다.</b> 지금 이 잡이 스스로 멈추는 자리가
     * 넷 있는데(이력 짝 불일치 · 재고 행 누락 · 재고 부족 · {@code asOf} 가 미래)
     * 넷 다 예외가 아니라 {@code FAILED} 상태로만 온다.
     */
    @Test
    @DisplayName("잡이 실패로 끝나면 원인과 함께 ERROR 로 남긴다")
    void reportsFailedJobAsError() {
        JobExecution failed = execution(BatchStatus.FAILED,
                new IllegalStateException("만료 이력 수가 만료 건수와 다릅니다"));

        scheduler(operatorReturning(failed)).expire();

        assertThat(only(Level.ERROR))
                .as("실패 원인이 이 로그 한 줄에 같이 있어야 알림만 보고 판단할 수 있다")
                .contains("만료 이력 수가 만료 건수와 다릅니다")
                .contains("FAILED");
    }

    /**
     * <b>사람이 멈춘 것은 사건이 아니다.</b> {@code STOPPED} 를 실패로 보면 배포·점검 때마다
     * 알림이 울린다.
     */
    @Test
    @DisplayName("사람이 멈춘 실행은 실패로 보지 않는다")
    void doesNotTreatStoppedAsFailure() {
        scheduler(operatorReturning(execution(BatchStatus.STOPPED, null))).expire();

        assertThat(logs.list).isEmpty();
    }

    /**
     * <b>주기보다 오래 걸린다는 신호다.</b> 한 번은 넘어가도 계속되면 봐야 하므로 WARN 이다 —
     * ERROR 로 두면 백로그가 큰 날 알림이 288번 울린다.
     */
    @Test
    @DisplayName("앞 실행이 아직 돌면 WARN 으로 건너뛴다")
    void warnsWhenPreviousRunIsStillGoing() {
        scheduler(operatorThrowing(
                new JobExecutionAlreadyRunningException("앞 실행이 돈다"))).expire();

        assertThat(only(Level.WARN)).contains("건너뜁니다");
    }

    /**
     * <b>중복 방지가 제 일을 한 것이다.</b> 같은 분에 두 번 뜨면 뒤엣것이 이 갈래로 온다 —
     * 막았다는 뜻이므로 ERROR 가 아니다.
     */
    @Test
    @DisplayName("이미 끝난 asOf 는 INFO 로 건너뛴다")
    void informsWhenAsOfAlreadyDone() {
        scheduler(operatorThrowing(
                new JobInstanceAlreadyCompleteException("이미 끝났다"))).expire();

        assertThat(only(Level.INFO)).contains("건너뜁니다");
    }

    /**
     * <b>시작조차 못 한 것은 사람이 봐야 한다.</b> DB 가 안 붙는 상황이 여기로 온다.
     */
    @Test
    @DisplayName("시작하지 못하면 ERROR 로 남긴다")
    void reportsStartFailureAsError() {
        scheduler(operatorThrowing(new IllegalStateException("DB 가 안 붙는다"))).expire();

        assertThat(only(Level.ERROR)).contains("시작하지 못했습니다");
    }

    /**
     * <b>기준 시각을 크론 슬롯에서 뽑는다.</b>
     *
     * <p>예전에는 분 단위로 잘랐다. 그러면 <b>실행된 시각</b>이 기준이라, 노드마다 밀리는
     * 정도가 다르면 {@code asOf} 가 갈려 중복 방지가 아예 발동하지 않는다.
     * 슬롯에서 뽑으면 <b>늦게 떠도 같은 슬롯이면 같은 값</b>이다.
     */
    @Test
    @DisplayName("기준 시각이 크론 슬롯이다 — 늦게 떠도 같은 값")
    void usesTheCronSlotAsAsOf() {
        JobParameters[] used = new JobParameters[1];
        scheduler(operator((job, params) -> {
            used[0] = params;
            return execution(BatchStatus.COMPLETED, null);
        })).expire();

        assertThat(used[0].getLocalDateTime("asOf"))
                .as("09:03:27 에 떠도 일 1회 크론의 슬롯은 그날 04:10 이다. 실행 시각을 쓰면 "
                        + "09:03 이 되어, 04:10 에 제때 뜬 노드와 값이 갈린다")
                .isEqualTo(EXPECTED_SLOT);
    }

    /**
     * <b>슬롯이 같으면 값이 같다 — 그것이 이 방식의 전부다.</b> 한 노드는 슬롯 직후에,
     * 다른 노드는 앞 실행이 밀려 한참 뒤에 떠도 같은 {@code asOf} 를 만들어야
     * {@code JOB_INST_UN} 이 둘 중 하나를 거부할 수 있다.
     */
    @Test
    @DisplayName("같은 슬롯 안에서는 언제 떠도 asOf 가 같다")
    void keepsTheSameAsOfAcrossTheWholeSlot() {
        LocalDateTime early = EXPECTED_SLOT.plusSeconds(1);
        // 다음 슬롯 직전은 안 쓴다 — 1초 차이면 조기 발화 관용 폭에 걸려 다음 슬롯이 된다.
        LocalDateTime late = EXPECTED_SLOT.plusHours(19);

        assertThat(asOfAt(early))
                .as("일 1회 크론이라 슬롯 하나가 하루다. 그 안에서 언제 뜨든 같아야 "
                        + "두 노드가 같은 JobInstance 를 노린다")
                .isEqualTo(asOfAt(late))
                .isEqualTo(EXPECTED_SLOT);
    }

    /** 주어진 시각에 떴을 때 스케줄러가 만드는 {@code asOf}. */
    private LocalDateTime asOfAt(LocalDateTime at) {
        JobParameters[] used = new JobParameters[1];
        Clock fixed = Clock.fixed(at.atZone(ZoneId.systemDefault()).toInstant(),
                ZoneId.systemDefault());
        new ExpireScheduler(operator((job, params) -> {
            used[0] = params;
            return execution(BatchStatus.COMPLETED, null);
        }), EXPIRE_JOB, new TimeProvider(fixed), EXPIRE_CRON, noVerifyRunning(), MAX_SKIPS, SLA_SECONDS, 60_000L, 600L).expire();
        return used[0].getLocalDateTime("asOf");
    }

    /**
     * <b>{@code IllegalStateException} 은 두 뜻으로 온다.</b> 인스턴스 생성이 READ COMMITTED 라
     * 진 쪽의 SELECT 가 안 막히고, 상대가 이미 커밋했으면 1062 대신
     * {@code Assert.state("JobInstance must not already exist")} 로 온다 — 그것은 중복 방지가
     * 일한 것이라 사건이 아니다.
     *
     * <p>그런데 같은 타입이 커넥션 문제 같은 <b>진짜 실패</b>로도 온다. 타입만 보고 INFO 로
     * 내렸다가 위 "시작하지 못하면 ERROR" 테스트가 그것을 잡았다. 그래서 인스턴스가 정말
     * 생겼는지 물어서 가른다 — 이 테스트가 그 갈래의 반대쪽이다.
     */
    @Test
    @DisplayName("인스턴스가 이미 있으면 IllegalStateException 도 INFO 다")
    void treatsLostRaceAsInfo() {
        JobOperator operator = (JobOperator) Proxy.newProxyInstance(
                JobOperator.class.getClassLoader(),
                new Class<?>[] {JobOperator.class},
                (proxy, method, args) -> {
                    if ("start".equals(method.getName()) && args.length == 2) {
                        throw new IllegalStateException("JobInstance must not already exist");
                    }
                    if ("getJobInstance".equals(method.getName())) {
                        // 다른 노드가 이미 만들어 둔 상태
                        return new JobInstance(1L, "expireJob");
                    }
                    throw new UnsupportedOperationException(method.getName());
                });

        scheduler(operator).expire();

        assertThat(only(Level.INFO))
                .as("ERROR 로 내보내면 배치를 두 대로 늘리는 순간 하루 288번 울린다")
                .contains("다른 노드가 같은 asOf 를 이미 시작했습니다");
    }

    /**
     * <b>실패 원인을 가르는 조회도 실패할 수 있다.</b> 그리고 하필 <b>가장 흔한 경우</b>가
     * 그렇다 — 원래 실패가 커넥션 문제면 인스턴스 조회도 같이 죽는다.
     *
     * <p>그때 예외를 그대로 올리면 뒤의 {@code catch (Exception)} 이 못 잡고
     * {@code expire()} 밖으로 나간다. 이 메서드가 절대 하지 않기로 한 일이다 —
     * 스프링이 로그만 남기고 다음 주기를 계속 잡으므로 <b>재고를 쓰는 유일한 잡이
     * 조용히 안 도는 상태</b>가 된다.
     */
    @Test
    @DisplayName("원인을 가르는 조회까지 실패해도 예외가 밖으로 안 나간다")
    void doesNotEscapeWhenTheDisambiguatingLookupAlsoFails() {
        JobOperator operator = (JobOperator) Proxy.newProxyInstance(
                JobOperator.class.getClassLoader(),
                new Class<?>[] {JobOperator.class},
                (proxy, method, args) -> {
                    if ("start".equals(method.getName()) && args.length == 2) {
                        throw new IllegalStateException("커넥션이 안 붙는다");
                    }
                    if ("getJobInstance".equals(method.getName())) {
                        throw new IllegalStateException("조회도 같은 이유로 죽는다");
                    }
                    throw new UnsupportedOperationException(method.getName());
                });

        // 던지면 여기서 테스트가 실패한다 — 그것이 이 테스트의 첫 번째 단언이다.
        scheduler(operator).expire();

        assertThat(logs.list)
                .as("가르지 못했다는 사실(WARN)과 원래 실패(ERROR) 둘 다 남아야 "
                        + "운영자가 무엇을 볼지 정할 수 있다")
                .hasSize(2);

        ILoggingEvent warn = logs.list.get(0);
        assertThat(warn.getLevel()).isEqualTo(Level.WARN);
        assertThat(warn.getFormattedMessage())
                .as("**두 원인을 한 줄에 남긴다.** 따로 두면 어느 쪽이 먼저인지 로그에서 안 보인다")
                .contains("가르지 못했습니다")
                .contains("커넥션이 안 붙는다")
                .contains("조회도 같은 이유로 죽는다");

        ILoggingEvent error = logs.list.get(1);
        assertThat(error.getLevel())
                .as("모를 때는 사건 쪽으로 기운다 — 진짜 실패를 INFO 로 삼키는 것이 더 나쁘다")
                .isEqualTo(Level.ERROR);
        assertThat(error.getThrowableProxy())
                .as("**원래 실패가 ERROR 에 실려 있어야 한다.** 레벨만 보면 예외를 빠뜨리는 "
                        + "변경이 그대로 통과하고, 운영자는 스택 없이 한 줄만 받는다")
                .isNotNull()
                .extracting(ThrowableProxy -> ThrowableProxy.getMessage())
                .isEqualTo("커넥션이 안 붙는다");
    }

    /** 남은 로그가 정확히 하나이고 기대한 레벨인지 확인한 뒤 본문을 돌려준다. */
    private String only(Level level) {
        assertThat(logs.list)
                .as("갈래가 하나로 뭉쳐 있으면 여기서 개수가 어긋난다")
                .hasSize(1);
        ILoggingEvent event = logs.list.get(0);
        assertThat(event.getLevel()).isEqualTo(level);
        return event.getFormattedMessage();
    }

    private ExpireScheduler scheduler(JobOperator operator) {
        Clock fixed = Clock.fixed(NOW.atZone(ZoneId.systemDefault()).toInstant(),
                ZoneId.systemDefault());
        return new ExpireScheduler(operator, EXPIRE_JOB, new TimeProvider(fixed), EXPIRE_CRON,
                noVerifyRunning(), MAX_SKIPS, SLA_SECONDS, 60_000L, 600L);
    }

    private JobExecution execution(BatchStatus status, Throwable failure) {
        JobExecution execution = new JobExecution(
                1L, new JobInstance(1L, "expireJob"), new JobParameters());
        execution.setStatus(status);
        if (failure != null) {
            execution.addFailureException(failure);
        }
        return execution;
    }

    private JobOperator operatorReturning(JobExecution execution) {
        return operator((job, params) -> execution);
    }

    private JobOperator operatorThrowing(Exception failure) {
        return operator((job, params) -> {
            throw failure;
        });
    }

    /**
     * {@code start(Job, JobParameters)} 만 갈아 끼운다. 나머지는 부르면 터지게 두어,
     * 스케줄러가 몰래 다른 것을 쓰기 시작하면 그 자리에서 드러나게 한다.
     */
    /**
     * <b>검증이 안 도는 상태.</b> 이 클래스의 축은 만료 쪽 로그 갈래라, 반대 방향 가드는 늘
     * 통과시킨다. 그 가드 자체는 {@code ExpireSchedulerVerifyGuardTest} 가 따로 잰다.
     */
    private static RunningJobProbe noVerifyRunning() {
        JobRepository repository = (JobRepository) Proxy.newProxyInstance(
                JobRepository.class.getClassLoader(),
                new Class<?>[] {JobRepository.class},
                (proxy, method, args) ->
                        "findRunningJobExecutions".equals(method.getName()) ? Set.of() : null);
        return new RunningJobProbe(repository, 1_800_000L, 600_000L, 120_000L, 120_000L);
    }

    private JobOperator operator(StartBehavior behavior) {
        return (JobOperator) Proxy.newProxyInstance(
                JobOperator.class.getClassLoader(),
                new Class<?>[] {JobOperator.class},
                (proxy, method, args) -> {
                    if ("start".equals(method.getName()) && args.length == 2) {
                        return behavior.start((Job) args[0], (JobParameters) args[1]);
                    }
                    if ("getJobInstance".equals(method.getName())) {
                        // 스케줄러가 IllegalStateException 을 "중복 방지가 일한 것" 과
                        // "진짜 실패" 로 가를 때 이것을 묻는다. 여기서는 아무도 인스턴스를
                        // 안 만들었으므로 없다 — 즉 진짜 실패 쪽이다.
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    @FunctionalInterface
    private interface StartBehavior {
        JobExecution start(Job job, JobParameters parameters) throws Exception;
    }

    /**
     * <b>동기 전제가 깨진 것을 알리는 유일한 자리다.</b> 비동기 {@code TaskExecutor} 가 물리면
     * {@code start} 가 즉시 {@code STARTED} 를 돌려주는데, {@code isUnsuccessful()} 은 그것을
     * 실패로 보지 않는다. 잡아 두지 않으면 <b>크론의 겹침 방지가 사라진 것을 아무도 모른다.</b>
     */
    @Test
    @DisplayName("비종단 상태로 돌아오면 ERROR 로 남긴다 — 동기 전제가 깨진 것이다")
    void reportsAsyncStartAsError() {
        scheduler(operatorReturning(execution(BatchStatus.STARTED, null))).expire();

        assertThat(only(Level.ERROR))
                .as("이 갈래가 없으면 겹침 방지가 사라진 채로 조용히 돈다")
                .contains("비동기")
                .contains("STARTED");
    }
}

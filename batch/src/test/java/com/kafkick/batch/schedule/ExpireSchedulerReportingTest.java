// 스케줄러가 상황을 어떤 레벨로 남기는지 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
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

    /** <b>기준 시각을 분 단위로 자른다.</b> 초가 남으면 중복 방지가 아예 안 걸린다. */
    @Test
    @DisplayName("기준 시각의 초를 버린다")
    void truncatesAsOfToMinute() {
        JobParameters[] used = new JobParameters[1];
        scheduler(operator((job, params) -> {
            used[0] = params;
            return execution(BatchStatus.COMPLETED, null);
        })).expire();

        assertThat(used[0].getLocalDateTime("asOf"))
                .as("09:03:27 로 띄워도 09:03:00 이어야 같은 분의 두 번째 실행이 막힌다")
                .isEqualTo(NOW.withSecond(0));
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
        return new ExpireScheduler(operator, null, new TimeProvider(fixed));
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
    private JobOperator operator(StartBehavior behavior) {
        return (JobOperator) Proxy.newProxyInstance(
                JobOperator.class.getClassLoader(),
                new Class<?>[] {JobOperator.class},
                (proxy, method, args) -> {
                    if ("start".equals(method.getName()) && args.length == 2) {
                        return behavior.start((Job) args[0], (JobParameters) args[1]);
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

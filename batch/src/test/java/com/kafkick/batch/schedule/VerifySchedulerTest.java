// 검증 스케줄러의 배선과 실패 분기를 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.test.util.ReflectionTestUtils;

import com.kafkick.batch.config.VerifyExecutorConfig;
import com.kafkick.batch.config.VerifyRunContext;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>검증이 조용히 안 도는 것이 가장 나쁜 결말이다.</b> 게이트 판정의 근거가 이 잡의 산출물인데,
 * 안 돌아도 데이터는 어제 그대로라 <b>아무도 이상을 못 느낀다.</b> 그래서 이 클래스가 재는 것은
 * <i>"판정이 맞는가"</i>({@code VerifyJobConfigTest} 의 몫)가 아니라 <b>뜨는 경로가 온전한가</b>다 —
 * 어느 실행기에 물렸는가, 어떤 파라미터를 싣는가, 실패가 밖으로 새는가, 잘못된 설정을
 * 기동 때 잡는가.
 *
 * <p><b>속성 집합을 {@code CleanupSchedulerTest} 와 글자 그대로 맞춰 둔다.</b> 스프링 테스트
 * 컨텍스트 캐시는 병합된 설정이 키라서, 한 글자만 달라도 컨텍스트가 새로 뜨고
 * <b>Testcontainers MySQL 이 하나 더 뜬다</b>.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // step-timeout(600000)보다 커야 한다 — RunningJobProbe 생성자가 검사한다.
        "batch.stuck-job-after-ms=1800000"
})
@Import(MySqlContainerConfig.class)
class VerifySchedulerTest {

    /** 검증 크론이 하루 한 번이라 최대 간격은 86,400초다. 여유 한 시간을 얹어 25시간. */
    private static final long DAILY_SLA_SECONDS = 90_000L;

    private static final String DAILY_CRON = "0 0 5 * * *";

    /** 고정 시계가 가리키는 날의 05:00 슬롯. {@code asOf} 단언의 기대값이다. */
    private static final LocalDateTime EXPECTED_SLOT = LocalDateTime.of(2026, 4, 1, 5, 0);

    @Autowired
    @Qualifier("verifyJob")
    private Job verifyJob;

    /** 클래스 최상단 속성으로 뜬 컨텍스트다 — {@code batch.scheduling.enabled=false} 다. */
    @Autowired
    private ApplicationContext context;

    private ListAppender<ILoggingEvent> logs;
    private ch.qos.logback.classic.Logger schedulerLog;
    private Level originalLevel;

    /**
     * 실패 분기는 <b>로그로만</b> 관측된다 — {@code verify()} 는 어떤 경우에도 예외를 안
     * 던지므로, 레벨과 문구를 안 재면 <i>"조용히 안 도는 상태"</i> 를 만든 변경이 통과한다.
     */
    @BeforeEach
    void captureLogs() {
        logs = new ListAppender<>();
        logs.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        logs.start();
        schedulerLog = (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger(VerifyScheduler.class);
        originalLevel = schedulerLog.getLevel();
        schedulerLog.setLevel(Level.TRACE);
        schedulerLog.addAppender(logs);
    }

    @AfterEach
    void releaseLogs() {
        schedulerLog.detachAppender(logs);
        schedulerLog.setLevel(originalLevel);
        logs.stop();
    }

    @Test
    @DisplayName("꺼져 있으면 스케줄러 빈이 아예 없다")
    void schedulerBeanIsAbsentWhenDisabled() {
        assertThat(context.getBeanNamesForType(VerifyScheduler.class))
                .as("메서드 안에서 검사하고 빠져나오는 방식이면, 그 검사를 안 넣은 경로가 "
                        + "나중에 하나 생기는 순간 조용히 돈다")
                .isEmpty();
    }

    /**
     * <b>게이지와 같은 조합을 돌아야 한다.</b> 크론이 다른 {@code (dataset, scope)} 를 돌면
     * {@code cy_verify_last_success_seconds} 는 영원히 비어 있고, {@code VerifyNeverSucceeded}
     * 가 배포 첫날부터 운다 — 배치는 매일 도는데 관제는 <i>"한 번도 안 돌았다"</i> 를 읽는다.
     *
     * <p><b>{@code asOf} 는 슬롯이어야 한다.</b> 원값({@code now()})을 실으면 노드마다 값이
     * 갈려 {@code JOB_INST_UN} 이 아무것도 거부하지 못하고, 배치를 두 대로 늘리는 날 둘이
     * 서로를 {@code DATASET_MUTATED_DURING_RUN} 으로 죽인다.
     */
    @Test
    @DisplayName("크론이 싣는 파라미터가 게이지가 보는 조합·슬롯과 같다")
    void carriesTheParametersTheGaugeWatches() {
        AtomicReference<JobParameters> captured = new AtomicReference<>();

        scheduler(BatchStatus.COMPLETED, null, captured).verify();

        JobParameters parameters = captured.get();
        assertThat(parameters.getString("dataset"))
                .as("게이트가 보는 조합이다 — VerifyRunContext 가 게이지와 함께 쓰는 값")
                .isEqualTo(VerifyRunContext.SLA_DATASET);
        assertThat(parameters.getString("scope")).isEqualTo(VerifyRunContext.SLA_SCOPE);
        assertThat(parameters.getLocalDateTime("asOf"))
                .as("발화 시각이 아니라 크론 슬롯이어야 노드가 늘어도 값이 같다")
                .isEqualTo(EXPECTED_SLOT);
        assertThat(parameters.getLong("attempt"))
                .as("슬롯마다 asOf 가 달라 1 로 충분하다. 시드가 점유한 것은 시드 자신의 asOf 다")
                .isEqualTo(1L);
    }

    /**
     * 원인 대부분은 <i>"그 {@code asOf} 로는 이제 못 잰다"</i> 다 — 그 슬롯에 발급이 있었다는
     * 뜻이고 재시도가 뜻이 없다. 그래서 <b>무엇을 해야 하는지가 로그에 있어야 한다.</b>
     */
    @Test
    @DisplayName("잡이 실패로 끝나도 예외가 밖으로 안 나가고, 원인을 로그가 말한다")
    void swallowsFailedJobStatus() {
        assertThatCode(() -> scheduler(BatchStatus.FAILED, null, null).verify())
                .doesNotThrowAnyException();

        assertThat(onlyError())
                .contains("검증 배치가 판정을 내지 못했습니다")
                .as("재시도가 아니라 슬롯을 옮기는 것이 처방이다")
                .contains("batch.schedule.verify-cron");
    }

    @Test
    @DisplayName("start 가 던져도 예외가 밖으로 안 나간다")
    void swallowsStartFailure() {
        assertThatCode(() -> scheduler(null, new IllegalStateException("DB 가 없다"), null).verify())
                .doesNotThrowAnyException();

        assertThat(onlyError()).contains("검증 배치를 시작하지 못했습니다");
    }

    /**
     * 비동기 빈이 물리면 크론의 겹침 방지가 사라지고 <b>실패가 로그 한 줄 없이 사라진다</b> —
     * {@code isUnsuccessful()} 이 {@code STARTED} 를 실패로 안 보기 때문이다.
     */
    @Test
    @DisplayName("비동기로 떠도 스케줄러가 죽지 않는다")
    void survivesAsyncOperator() {
        assertThatCode(() -> scheduler(BatchStatus.STARTED, null, null).verify())
                .doesNotThrowAnyException();

        assertThat(onlyError())
                .as("겹침 방지가 사라진 상태라 그 사실을 그 자리에서 알려야 한다")
                .contains("비동기로 떴습니다");
    }

    /**
     * <b>주기와 SLA 를 한자리에서 맞춘다.</b> 안 맞으면 정상 상태에서 매일 critical 이 뜨고,
     * 그 알림이 무시되기 시작하면 진짜 검증 정지도 같은 알림으로 묻힌다.
     */
    @Test
    @DisplayName("크론을 늘려 SLA 를 넘기면 기동을 거절한다")
    void rejectsCronThatCannotMeetTheSla() {
        assertThatThrownBy(() -> build("0 0 5 * * SUN", DAILY_SLA_SECONDS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("verify-sla-seconds");
    }

    @Test
    @DisplayName("SLA 를 함께 올리면 같은 크론이 통과한다")
    void acceptsWeeklyCronWhenTheSlaIsRaised() {
        // 주 1회면 최대 간격이 604,800초다. 그 위에 여유를 두면 성립한다.
        assertThatCode(() -> build("0 0 5 * * SUN", 700_000L)).doesNotThrowAnyException();
    }

    /**
     * <b>기본 조합이 부등식을 만족한다.</b> 일 1회(86,400) + 되읽기 한 주기(60)
     * + {@code VerifyRunningTooLong}(1,200) = <b>87,660</b> 이 SLA 25시간(90,000) 아래다.
     * 여유가 <b>2,340초</b>뿐이라 {@code run-refresh-ms} 를 올리거나 크론을 늘리면 곧바로
     * 위 거절 분기로 넘어간다 — 그 사실을 값으로 남긴다.
     *
     * <p>잡 소요 항이 셋 중 가장 큰데, 검증이 실측 472초로 가장 무거운 잡이라서다.
     * 그 항 자체는 {@code SlaBudgetTest} 가 따로 잰다 — 여기서는 <b>기본값 조합이
     * 통과한다</b>는 것만 본다.
     */
    @Test
    @DisplayName("기본 크론과 기본 SLA 가 실제로 맞물린다")
    void defaultCronFitsTheDefaultSla() {
        assertThatCode(() -> build(DAILY_CRON, DAILY_SLA_SECONDS)).doesNotThrowAnyException();
    }

    /**
     * 끄는 수단은 하나여야 한다. {@code "-"} 로 끄면 트리거만 죽고 알림은 그대로 살아
     * SLA 를 넘긴 뒤부터 영원히 운다 — 끈 것을 아무도 알림에 말해 주지 않는다.
     */
    @Test
    @DisplayName("크론을 \"-\" 로 끄는 것은 거절한다")
    void rejectsDisabledCron() {
        assertThatThrownBy(() -> build("-", DAILY_SLA_SECONDS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.scheduling.enabled=false");
    }

    private VerifyScheduler scheduler(BatchStatus status, RuntimeException failure,
            AtomicReference<JobParameters> captured) {
        return new VerifyScheduler(stubOperator(status, failure, captured), verifyJob,
                utcClock(), DAILY_CRON, DAILY_SLA_SECONDS, 60_000L);
    }

    private static JobOperator stubOperator(BatchStatus status, RuntimeException failure,
            AtomicReference<JobParameters> captured) {
        return (JobOperator) Proxy.newProxyInstance(
                JobOperator.class.getClassLoader(),
                new Class<?>[] {JobOperator.class},
                (proxy, method, args) -> {
                    if (!"start".equals(method.getName())) {
                        return null;
                    }
                    if (captured != null) {
                        captured.set((JobParameters) args[1]);
                    }
                    if (failure != null) {
                        throw failure;
                    }
                    JobExecution execution = new JobExecution(
                            1L, new JobInstance(1L, VerifyRunContext.JOB_NAME),
                            new JobParameters());
                    execution.setStatus(status);
                    return execution;
                });
    }

    /** 협력자에 {@code null} 을 안 넘긴다 — 쓰는 코드가 생기는 날 엉뚱한 자리에서 NPE 가 난다. */
    private VerifyScheduler build(String cron, long slaSeconds) {
        return new VerifyScheduler(stubOperator(BatchStatus.COMPLETED, null, null), verifyJob,
                utcClock(), cron, slaSeconds, 60_000L);
    }

    /** ERROR 갈래가 하나여야 한다 — 뭉쳐 있으면 여기서 개수가 어긋난다. */
    private String onlyError() {
        List<ILoggingEvent> errors = logs.list.stream()
                .filter(event -> event.getLevel() == Level.ERROR)
                .toList();
        assertThat(errors).hasSize(1);
        return errors.get(0).getFormattedMessage();
    }

    /**
     * <b>05:00 슬롯을 이미 지난 시각이어야 한다.</b> {@code CronSlot.atOrBefore} 는 그 시각
     * 이전의 마지막 슬롯을 주므로, 05:00 앞으로 잡으면 <b>전날</b> 슬롯이 나와
     * {@code EXPECTED_SLOT} 단언이 날짜에서 어긋난다.
     */
    private static TimeProvider utcClock() {
        return new TimeProvider(Clock.fixed(
                LocalDateTime.of(2026, 4, 1, 6, 0).atZone(ZoneId.of("UTC")).toInstant(),
                ZoneId.of("UTC")));
    }

    /**
     * <b>공용(동기) 실행기에 물려야 한다.</b> 비동기 빈이 물리면 겹침 방지가 사라지고, 8분짜리
     * 잡의 실패가 로그 한 줄 없이 사라진다. 빈이 둘이라 이름 기반 주입이 흔들리는 날
     * <b>기동은 성공하고 05:00 만 이상해진다</b> — 그래서 배선을 직접 본다.
     *
     * <p>속성 집합은 {@code CleanupSchedulerTest.WhenEnabled} 와 글자 그대로 같다(컨텍스트 재사용).
     */
    @Nested
    @SpringBootTest(properties = {
            "spring.batch.job.enabled=false",
            "batch.scheduling.enabled=true",
            // 크론을 먼 미래로 밀어 테스트 중에 실제로 돌지 않게 한다. 빈 존재만 본다.
            "batch.schedule.expire-cron=0 0 0 1 1 *",
            // 정리 크론도 함께 민다. 그러지 않으면 04:30 UTC(13:30 KST)를 지나며 도는 CI 에서
            // 진짜 정리가 발화해 asof_state · verification_findings · 통계 세 테이블을 지운다 —
            // MySqlContainerConfig 는 컨테이너를 공유하므로 무관한 검증 테스트가 그날만 빨개진다.
            // 연 1회 크론은 CleanupScheduler 의 SLA 가드에 걸리므로 SLA 도 함께 올린다.
            "batch.schedule.cleanup-cron=0 0 0 1 1 *",
            "batch.metrics.cleanup-sla-seconds=999999999",
            "batch.metrics.expire-sla-seconds=999999999",
            // 검증 크론도 함께 민다(CY-470). 기본값 05:00 UTC 를 그대로 두면
            // 그 시각을 지나며 도는 CI 에서 진짜 검증이 발화해, 공유 컨테이너의
            // asof_state 를 300만 행까지 채우고 다른 테스트의 전제를 바꾼다 —
            // 위 정리 크론을 민 것과 같은 이유다. 연 1회는 SLA 가드에 걸려 SLA 도 올린다.
            "batch.schedule.verify-cron=0 0 0 1 1 *",
            "batch.metrics.verify-sla-seconds=999999999"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("켜져 있을 때")
    class WhenEnabled {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("스케줄러 빈이 있다")
        void schedulerBeanExists() {
            assertThat(context.getBeanNamesForType(VerifyScheduler.class))
                    .as("꺼진 쪽만 확인하면 '항상 없다' 로도 통과한다")
                    .hasSize(1);
        }

        @Test
        @DisplayName("공용(동기) JobOperator 에 물린다 — verify 전용 비동기 빈이 아니다")
        void usesTheSharedSynchronousOperator() {
            VerifyScheduler scheduler = context.getBean(VerifyScheduler.class);
            Object wired = ReflectionTestUtils.getField(scheduler, "jobOperator");

            assertThat(wired).isSameAs(context.getBean("jobOperator"));
            assertThat(wired)
                    .as("비동기 빈이 물리면 실패가 로그 한 줄 없이 사라진다")
                    .isNotSameAs(context.getBean(VerifyExecutorConfig.OPERATOR));
        }
    }
}

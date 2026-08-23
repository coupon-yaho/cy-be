// 정리 스케줄러의 배선과 실패 분기를 확인합니다.
package com.kafkick.batch.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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
import com.kafkick.core.support.TimeProvider;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>정리는 조용히 안 도는 것이 가장 나쁜 결말이다.</b> 그래서 이 클래스가 재는 것은
 * <i>"지웠는가"</i>({@code CleanupJobTest} 의 몫)가 아니라 <b>뜨는 경로가 온전한가</b>다 —
 * 어느 실행기에 물렸는가, 실패가 밖으로 새는가, 잘못된 설정을 기동 때 잡는가.
 *
 * <p><b>속성 집합을 {@code ExpireSchedulerVerifyGuardTest}·{@code ExpireSchedulerTest} 와
 * 글자 그대로 맞춰 둔다.</b> 스프링 테스트 컨텍스트 캐시는 병합된 설정이 키라서, 한 글자만
 * 달라도 컨텍스트가 새로 뜨고 <b>Testcontainers MySQL 이 하나 더 뜬다</b>(그 누적이 CI 를
 * 메모리로 죽인 적이 있다 — {@code spring.test.context.cache.maxSize} 가 그 흔적이다).
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        // step-timeout(600000)보다 커야 한다 — RunningJobProbe 생성자가 검사한다.
        "batch.stuck-job-after-ms=1800000"
})
@Import(MySqlContainerConfig.class)
class CleanupSchedulerTest {

    /** 정리 크론이 하루 한 번이라 최대 간격은 86,400초다. */
    private static final long DAILY_SLA_SECONDS = 90_000L;

    private static final String DAILY_CRON = "0 30 4 * * *";

    @Autowired
    @Qualifier("cleanupJob")
    private Job cleanupJob;

    /**
     * <b>{@code JobOperator.start} 는 잡이 실패로 끝나도 예외를 안 던진다</b> — 실행 결과를
     * 돌려줄 뿐이다. 그래서 스케줄러가 상태를 직접 보는데, 그 분기가 안 재지면
     * <i>"매일 실패하는데 로그도 조용한"</i> 상태가 열린다.
     */
    @Test
    @DisplayName("잡이 실패로 끝나도 예외가 밖으로 안 나간다")
    void swallowsFailedJobStatus() {
        assertThatCode(() -> scheduler(BatchStatus.FAILED, null).cleanup())
                .doesNotThrowAnyException();
    }

    /**
     * {@code @Scheduled} 에서 예외가 나가면 스프링이 로그만 남기고 다음 주기를 잡는다 —
     * 그러면 정리가 <b>조용히 안 도는 상태</b>가 된다.
     */
    @Test
    @DisplayName("start 가 던져도 예외가 밖으로 안 나간다")
    void swallowsStartFailure() {
        assertThatCode(() -> scheduler(null, new IllegalStateException("DB 가 없다")).cleanup())
                .doesNotThrowAnyException();
    }

    /**
     * 비동기 빈이 물리면 크론의 겹침 방지가 사라진다. 그 상태를 예외로 만들 수는 없으므로
     * (이미 잡이 떴다) 최소한 <b>스케줄러가 죽지 않고 ERROR 로 남기는지</b>를 잰다.
     */
    @Test
    @DisplayName("비동기로 떠도 스케줄러가 죽지 않는다")
    void survivesAsyncOperator() {
        assertThatCode(() -> scheduler(BatchStatus.STARTED, null).cleanup())
                .doesNotThrowAnyException();
    }

    /**
     * <b>주기와 SLA 를 한자리에서 맞춘다.</b> 안 맞으면 정상 상태에서 매일 warning 이 뜨고,
     * 그 알림이 무시되기 시작하면 진짜 정리 정지도 같은 알림으로 묻힌다.
     */
    @Test
    @DisplayName("크론을 늘려 SLA 를 넘기면 기동을 거절한다")
    void rejectsCronThatCannotMeetTheSla() {
        assertThatThrownBy(() -> build("0 30 4 * * SUN", DAILY_SLA_SECONDS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cleanup-sla-seconds");
    }

    @Test
    @DisplayName("SLA 를 함께 올리면 같은 크론이 통과한다")
    void acceptsWeeklyCronWhenTheSlaIsRaised() {
        // 주 1회면 최대 간격이 604,800초다. 그 위에 여유를 두면 성립한다.
        assertThatCode(() -> build("0 30 4 * * SUN", 700_000L)).doesNotThrowAnyException();
    }

    /**
     * 끄는 수단은 하나여야 한다. {@code "-"} 로 끄면 트리거만 죽고 알림은 그대로 살아
     * 25시간 뒤부터 영원히 운다 — 끈 것을 아무도 알림에 말해 주지 않는다.
     */
    @Test
    @DisplayName("크론을 \"-\" 로 끄는 것은 거절한다")
    void rejectsDisabledCron() {
        assertThatThrownBy(() -> build("-", DAILY_SLA_SECONDS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch.scheduling.enabled=false");
    }

    private CleanupScheduler scheduler(BatchStatus status, RuntimeException failure) {
        JobOperator stub = (JobOperator) Proxy.newProxyInstance(
                JobOperator.class.getClassLoader(),
                new Class<?>[] {JobOperator.class},
                (proxy, method, args) -> {
                    if (!"start".equals(method.getName())) {
                        return null;
                    }
                    if (failure != null) {
                        throw failure;
                    }
                    JobExecution execution = new JobExecution(
                            1L, new JobInstance(1L, "cleanupJob"), new JobParameters());
                    execution.setStatus(status);
                    return execution;
                });
        return new CleanupScheduler(stub, cleanupJob, utcClock(), DAILY_CRON,
                DAILY_SLA_SECONDS, 60_000L);
    }

    private CleanupScheduler build(String cron, long slaSeconds) {
        return new CleanupScheduler(null, cleanupJob, utcClock(), cron, slaSeconds, 60_000L);
    }

    private static TimeProvider utcClock() {
        return new TimeProvider(Clock.fixed(
                LocalDateTime.of(2026, 4, 1, 3, 0).atZone(ZoneId.of("UTC")).toInstant(),
                ZoneId.of("UTC")));
    }

    /**
     * <b>공용(동기) 실행기에 물려야 한다.</b> 비동기 빈이 물리면 크론의 겹침 방지가 사라지고,
     * 정리가 자기 자신과 겹쳐 같은 대상을 두 번 훑는다. 빈이 둘이라 이름 기반 주입이
     * 흔들리는 날 <b>기동은 성공하고 04:30 만 이상해진다</b> — 그래서 배선을 직접 본다.
     *
     * <p>속성 집합은 {@code ExpireSchedulerTest.WhenEnabled} 와 글자 그대로 같다(컨텍스트 재사용).
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
            "batch.metrics.expire-sla-seconds=999999999"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("켜져 있을 때")
    class WhenEnabled {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("스케줄러 빈이 있다")
        void schedulerBeanExists() {
            assertThat(context.getBeanNamesForType(CleanupScheduler.class))
                    .as("꺼진 쪽만 확인하면 '항상 없다' 로도 통과한다")
                    .hasSize(1);
        }

        @Test
        @DisplayName("공용(동기) JobOperator 에 물린다 — verify 전용 비동기 빈이 아니다")
        void usesTheSharedSynchronousOperator() {
            CleanupScheduler scheduler = context.getBean(CleanupScheduler.class);
            Object wired = ReflectionTestUtils.getField(scheduler, "jobOperator");

            assertThat(wired).isSameAs(context.getBean("jobOperator"));
            assertThat(wired)
                    .as("비동기 빈이 물리면 정리가 자기 자신과 겹친다")
                    .isNotSameAs(context.getBean(VerifyExecutorConfig.OPERATOR));
        }
    }

    /**
     * 끄는 스위치가 <b>빈 자체를 없애는지</b> 본다. 메서드 안에서 검사하고 빠져나오는
     * 방식이면 그 검사를 안 넣은 경로가 나중에 하나 생기는 순간 조용히 돈다.
     */
    @Test
    @DisplayName("꺼져 있으면 스케줄러 빈이 아예 없다")
    void schedulerBeanIsAbsentWhenDisabled() {
        assertThat(disabledContext.getBeanNamesForType(CleanupScheduler.class)).isEmpty();
    }

    @Autowired
    private ApplicationContext disabledContext;
}

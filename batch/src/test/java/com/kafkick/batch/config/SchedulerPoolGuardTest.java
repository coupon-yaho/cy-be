// 스케줄러 풀 하한 가드가 실제로 배선되고 작은 풀을 거절하는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code spring.task.scheduling.pool.size} 는 Boot 가 직접 소비해서 {@code @Value} 스캔에
 * 안 잡힌다.</b> {@code ResolvedBatchConfigTest} 가 해석된 값을 키 경로로 지키고
 * {@code VerificationMetricExposureTest} 가 스케줄러 빈의 코어 크기를 보지만, <b>둘 다
 * 운영에서 환경변수로 1 을 주는 경로는 못 막았다</b> — 기동은 성공하고 여덟
 * {@code @Scheduled} 가 스레드 하나를 다툰다(CodeRabbit 지적).
 *
 * <p>이 클래스가 재는 것은 <b>가드가 그 상태를 기동에서 끊는가</b>다.
 */
@SpringBootTest(properties = {
        "spring.batch.job.enabled=false",
        "batch.scheduling.enabled=false",
        "batch.stuck-job-after-ms=1800000"
})
@Import(MySqlContainerConfig.class)
class SchedulerPoolGuardTest {

    @Autowired
    private ApplicationContext context;

    /**
     * <b>인스턴스를 손으로 만들어 {@code run()} 을 부르는 것만으로는 부족하다.</b>
     * {@code @Component} 를 떼거나 컴포넌트 스캔 밖 패키지로 옮기면 <b>전 저장소가 초록인 채로
     * 가드가 안 돈다</b> — 나머지 테스트는 풀이 넉넉한 컨텍스트에서만 도니까.
     *
     * <p>순서도 함께 본다. {@code JobLauncherApplicationRunner} 의 정렬값이 0 이라 가드가
     * 그 뒤에 오면 잡이 먼저 뜨고 나서 말한다.
     */
    @Test
    @DisplayName("가드가 러너로 배선되고 잡보다 먼저 온다")
    void guardRunsBeforeAnyJob() {
        List<ApplicationRunner> runners = context.getBeanProvider(ApplicationRunner.class)
                .orderedStream()
                .toList();

        assertThat(runners)
                .as("빈이 없으면 이 가드는 아무 일도 안 한다")
                .anyMatch(SchedulerPoolGuard.class::isInstance);
        assertThat(runners.indexOf(runners.stream()
                .filter(SchedulerPoolGuard.class::isInstance)
                .findFirst()
                .orElseThrow()))
                .as("잡보다 뒤면 잡이 먼저 뜨고 나서 가드가 말한다")
                .isLessThan(runners.size());
    }

    /**
     * <b>수를 손으로 안 센다.</b> {@code .example} 이 <i>"@Scheduled 가 여덟이다"</i> 를 손으로
     * 적고 있는데, 여기서 또 세면 세어야 할 자리가 하나 더 는다. 등록된 태스크 수를 직접
     * 물으면 그 수가 코드와 자동으로 같아진다.
     */
    @Test
    @DisplayName("실제 등록된 @Scheduled 수를 풀이 받친다")
    void poolCoversTheRegisteredTaskCount() {
        int registered = context.getBean(ScheduledAnnotationBeanPostProcessor.class)
                .getScheduledTasks().size();
        int pool = context.getBean("taskScheduler", ThreadPoolTaskScheduler.class)
                .getScheduledThreadPoolExecutor().getCorePoolSize();

        assertThat(pool)
                .as("등록 %d 개가 스레드 %d 개를 다투면 한 작업이 도는 동안 나머지가 멈춘다",
                        registered, pool)
                .isGreaterThanOrEqualTo(registered);
    }

    /**
     * <b>이것이 이 가드의 존재 이유다.</b> 풀이 작으면 기동이 죽어야 한다 — 그냥 뜨면
     * 되읽기가 못 돌아 게이지가 낡고, 배치는 멀쩡한데 SLA 알림이 운다. 그리고 되읽기가
     * <i>실패</i>한 것도 아니라 {@code cy_batch_refresh_failures_total} 도 안 오른다.
     */
    @Test
    @DisplayName("풀이 @Scheduled 수보다 작으면 기동을 거절한다")
    void rejectsPoolSmallerThanTheTaskCount() {
        ScheduledAnnotationBeanPostProcessor tasks =
                context.getBean(ScheduledAnnotationBeanPostProcessor.class);

        ThreadPoolTaskScheduler tooSmall = new ThreadPoolTaskScheduler();
        tooSmall.setPoolSize(1);
        tooSmall.initialize();
        try {
            assertThatThrownBy(() -> new SchedulerPoolGuard(tooSmall, tasks).run(null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("spring.task.scheduling.pool.size")
                    .as("무엇을 얼마로 올려야 하는지가 메시지에 있어야 한다")
                    .hasMessageContaining("BATCH_SCHEDULER_POOL_SIZE");
        } finally {
            tooSmall.shutdown();
        }
    }

    /** 같은 수면 통과한다 — 여유를 요구하지 않는다. {@code .example} 이 정확히 그 수로 둔다. */
    @Test
    @DisplayName("풀이 @Scheduled 수와 같으면 통과한다")
    void acceptsPoolEqualToTheTaskCount() {
        ScheduledAnnotationBeanPostProcessor tasks =
                context.getBean(ScheduledAnnotationBeanPostProcessor.class);

        ThreadPoolTaskScheduler exact = new ThreadPoolTaskScheduler();
        exact.setPoolSize(Math.max(1, tasks.getScheduledTasks().size()));
        exact.initialize();
        try {
            assertThatCode(() -> new SchedulerPoolGuard(exact, tasks).run(null))
                    .doesNotThrowAnyException();
        } finally {
            exact.shutdown();
        }
    }

    /**
     * <b>스케줄러를 켠 컨텍스트가 진짜 판이다.</b> 위 클래스는
     * {@code batch.scheduling.enabled=false} 라 {@code @Scheduled} 가 셋 적게 등록된다 —
     * 그 상태만 재면 <b>운영에서 실제로 등록되는 수</b>를 한 번도 안 본다.
     *
     * <p>속성 집합은 {@code CleanupSchedulerTest.WhenEnabled} 와 글자 그대로 같다(컨텍스트 재사용).
     */
    @Nested
    @SpringBootTest(properties = {
            "spring.batch.job.enabled=false",
            "batch.scheduling.enabled=true",
            "batch.schedule.expire-cron=0 0 0 1 1 *",
            "batch.schedule.cleanup-cron=0 0 0 1 1 *",
            "batch.metrics.cleanup-sla-seconds=999999999",
            "batch.metrics.expire-sla-seconds=999999999",
            "batch.schedule.verify-cron=0 0 0 1 1 *",
            "batch.metrics.verify-sla-seconds=999999999"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("스케줄러를 켰을 때")
    class WhenEnabled {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("스케줄러 넷이 더 붙어도 풀이 받친다")
        void poolStillCoversEveryTask() {
            int registered = context.getBean(ScheduledAnnotationBeanPostProcessor.class)
                    .getScheduledTasks().size();
            int pool = context.getBean("taskScheduler", ThreadPoolTaskScheduler.class)
                    .getScheduledThreadPoolExecutor().getCorePoolSize();

            assertThat(pool)
                    .as("운영 형상이다 — 여기서 모자라면 되읽기가 배치에 밀린다. 등록=%d",
                            registered)
                    .isGreaterThanOrEqualTo(registered);
        }
    }
}

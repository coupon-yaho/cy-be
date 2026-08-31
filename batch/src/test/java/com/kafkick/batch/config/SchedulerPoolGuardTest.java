// 스케줄러 풀 하한 가드가 실제로 배선되고 작은 풀을 거절하는지 확인합니다.
package com.kafkick.batch.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.OrderUtils;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.kafkick.batch.observation.DomainGaugeRegistrar;
import com.kafkick.storage.db.MySqlContainerConfig;

/**
 * <b>{@code spring.task.scheduling.pool.size} 는 Boot 가 직접 소비해서 {@code @Value} 스캔에
 * 안 잡힌다.</b> {@code ResolvedBatchConfigTest} 가 해석된 값을 키 경로로 지키고
 * {@code VerificationMetricExposureTest} 가 스케줄러 빈의 코어 크기를 보지만, <b>둘 다
 * 운영에서 환경변수로 1 을 주는 경로는 못 막았다</b> — 기동은 성공하고 등록된
 * 스케줄 작업 열하나가 스레드 하나를 다툰다(CodeRabbit 지적).
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
                .hasAtLeastOneElementOfType(SchedulerPoolGuard.class);

        // ⚠️ **인덱스로 보면 안 된다.** 이 컨텍스트는 spring.batch.job.enabled=false 라
        //    JobLauncherApplicationRunner 빈이 아예 없다 — 그 빈을 찾아 인덱스를 비교하는
        //    방식이면 단언이 한 번도 안 돌고, `indexOf(...) < size()` 는 목록에 있는
        //    원소면 **언제나 참**이라 아무것도 안 지킨다(CodeRabbit 이 그 상태를 잡았다).
        //    SchemaPresenceGuard 가 같은 이유로 정렬값을 직접 본다.
        assertThat(OrderUtils.getOrder(SchedulerPoolGuard.class))
                .as("JobLauncherApplicationRunner 의 정렬값은 0 이다. @Order 를 떼면 "
                        + "LOWEST_PRECEDENCE 라 잡이 먼저 돌고, 풀이 모자란 채로 "
                        + "그 잡이 뜬다")
                .isNotNull()
                .isLessThan(0);
    }

    /**
     * <b>수를 손으로 안 센다.</b> {@code .example} 이 <i>"스케줄 작업이 열하나다"</i> 를 손으로
     * 적고 있는데, 여기서 또 세면 세어야 할 자리가 하나 더 는다. 등록된 태스크 수를 직접
     * 물으면 그 수가 코드와 자동으로 같아진다.
     */
    @Test
    @DisplayName("실제 등록된 작업 수를 풀이 받친다")
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
    @DisplayName("풀이 등록된 작업 수보다 작으면 기동을 거절한다")
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
    @DisplayName("풀이 등록된 작업 수와 같으면 통과한다")
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
     * {@code batch.scheduling.enabled=false} 라 그 스위치를 단 넷이 안 등록된다 —
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

    /**
     * <b>배포에 실리는 기본값을 CI 가 본 적이 없었다.</b>
     *
     * <p>{@link WhenEnabled#poolStillCoversEveryTask()} 는 등록 수를 직접 세어 낡지 않지만,
     * 그 컨텍스트의 풀은 테스트 리소스 {@code application.yml} 이 주는 16 이라 등록이 몇이든
     * 통과한다. 정작 컨테이너에 실리는 것은 {@code application.yml.example} 과
     * {@code batch.yml} 의 기본값이고, 그 둘을 등록 수와 맞춰 보는 검사가 없었다.
     *
     * <p><b>등록 수도 배포와 달랐다.</b> 관측 스위치 셋은 {@code .example} 에서 기본이
     * {@code true} 인데 {@code batch/src/test/resources/application.yml} 이 그 파일을 통째로
     * 가려서, 테스트 컨텍스트에서는 그 빈들이 아예 안 생긴다(등록 8 대 배포 11).
     * 그래서 이 중첩 클래스는 <b>스위치를 켜서 배포 형상으로 맞춘 뒤</b> 잰다.
     * 켜는 방식은 {@code ObservationAccountPrivilegeTest} 와 같다.
     *
     * <p><b>실제로 이 구멍으로 샜다.</b> CY-699 가 열한째 작업을 더했는데 기본값은 10 에
     * 머물렀고, 전 저장소가 초록인 채로 배포된 배치가 기동을 거절했다
     * ({@code 풀=10 등록=11}. 당시 메시지는 @Scheduled 라고 말했다). 열한째는 {@code SchedulingConfigurer} 로 붙어서
     * {@code @Scheduled} 전수 grep 에도 안 잡힌다 — 그래서 애노테이션을 세지 않고
     * <b>등록 수</b>와 맞춘다.
     */
    @Nested
    @SpringBootTest(properties = {
            "spring.batch.job.enabled=false",
            "batch.scheduling.enabled=true",
            "batch.schedule.expire-cron=0 0 0 1 1 *",
            "batch.schedule.cleanup-cron=0 0 0 1 1 *",
            "batch.schedule.verify-cron=0 0 0 1 1 *",
            "batch.metrics.cleanup-sla-seconds=999999999",
            "batch.metrics.expire-sla-seconds=999999999",
            "batch.metrics.verify-sla-seconds=999999999",
            // 배포 기본값이 true 인 셋. 테스트 리소스가 .example 을 가려서 안 켜지므로 여기서 켠다.
            "observation.datasource.enabled=true",
            "observation.domain-gauge.enabled=true",
            "observation.pending-issued-gauge.enabled=true",
            // 재는 것은 **등록**뿐이라 실제로 돌 필요가 없다. @SpringBootTest 컨텍스트는
            // 캐시되어 JVM 끝까지 살아 있으므로, 기본값(1초·30초)대로 두면 :batch:test 가
            // 도는 내내 공유 MySQL 을 초당 한 번씩 치고 Redis 없는 환경에서 연결 실패
            // 로그가 쌓여 다른 테스트의 실패 원인을 오독하게 만든다.
            "observation.domain-gauge.interval-ms=3600000",
            "observation.domain-gauge.aggregate-interval-ms=3600000",
            "observation.pending-issued-gauge.interval=1h"
    })
    @Import(MySqlContainerConfig.class)
    @DisplayName("배포 형상으로 띄웠을 때")
    class WhenShapedLikeDeployment {

        @Autowired
        private ApplicationContext context;

        @Test
        @DisplayName("배포 기본값이 등록 수를 받친다 — .example 과 batch.yml 둘 다")
        void shippedDefaultsCoverEveryTask() throws Exception {
            int registered = context.getBean(ScheduledAnnotationBeanPostProcessor.class)
                    .getScheduledTasks().size();

            assertThat(context.getBeansOfType(SchedulingConfigurer.class))
                    .as("열한째 작업이다. 이 빈이 없으면 아래 등록 수는 배포 형상이 아니라서 "
                            + "기본값이 모자라도 통과한다 — 이 검사의 전제가 무너진다")
                    .containsKey("pendingIssuedGaugeScheduler");
            assertThat(context.getBeansOfType(DomainGaugeRegistrar.class))
                    .as("도메인 게이지 둘. 위와 같은 이유로 형상을 못 박는다")
                    .isNotEmpty();

            assertThat(defaultFrom(
                    Path.of("src/main/resources/application.yml.example"),
                    "(?m)^\\s*size:\\s*\\$\\{BATCH_SCHEDULER_POOL_SIZE:(\\d+)\\}"))
                    .as("이미지에 실리는 기본값이다. 등록 %d 개보다 작으면 컨테이너가 "
                            + "SchedulerPoolGuard 에서 기동을 거절한다", registered)
                    .isEqualTo(registered);

            assertThat(defaultFrom(
                    Path.of("../batch.yml"),
                    "(?m)^\\s*BATCH_SCHEDULER_POOL_SIZE:\\s*\\$\\{BATCH_SCHEDULER_POOL_SIZE:-(\\d+)\\}"))
                    .as("compose 가 주는 기본값이다. .example 만 고치면 여기가 덮어써서 "
                            + "고친 값이 안 선다. 등록 %d 개", registered)
                    .isEqualTo(registered);
        }

        /** 파일에서 기본값 하나를 읽는다. 못 찾으면 단언 전에 실패시킨다 — 조용히 0 을 쓰지 않는다. */
        private int defaultFrom(Path path, String regex) throws Exception {
            String text = Files.readString(path);
            Matcher matcher = Pattern.compile(regex).matcher(text);
            assertThat(matcher.find())
                    .as("%s 에서 기본값을 못 찾았다. 키 표기가 바뀌었으면 이 정규식도 함께 "
                            + "고쳐야 한다 — 안 고치면 이 검사가 조용히 죽는다", path)
                    .isTrue();
            return Integer.parseInt(matcher.group(1));
        }
    }
}

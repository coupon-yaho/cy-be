// 스케줄러 풀이 등록된 스케줄 작업 수보다 작으면 기동을 거절합니다.
package com.kafkick.batch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

/**
 * <b>{@code spring.task.scheduling.pool.size} 는 Boot 가 직접 소비해서 우리 코드가 안 읽는다.</b>
 * 그래서 키 경로가 죽거나 운영이 {@code BATCH_SCHEDULER_POOL_SIZE=1} 을 주면 <b>아무 데서도
 * 안 드러난다</b> — 기동은 성공하고, 등록된 작업 열하나가 스레드 하나를 다툰다.
 *
 * <p><b>그 상태가 왜 나쁜가.</b> 검증이 한 번에 8분(실측 472초)을 잡고 있는데, 그동안 되읽기
 * 넷이 못 돌면 게이지가 그만큼 낡는다. 그리고 그 게이지가 SLA 알림의 근거다 —
 * <b>배치는 멀쩡한데 알림이 운다.</b> 되읽기가 <i>실패</i>한 것도 아니라
 * {@code cy_batch_refresh_failures_total} 도 안 오른다: 실행 자체가 안 된 것이다.
 *
 * <h2>왜 상수로 안 세나</h2>
 *
 * <p>{@code application.yml.example} 이 <i>"스케줄 작업이 열하나다"</i> 를 손으로 세어
 * 적고 있고, 그 수가 코드와 함께 움직여야 신호가 산다. 여기서 또 하나를 손으로 적으면
 * <b>세어야 할 자리가 하나 더 는다</b> — CY-446 이 그 신호를 한 번 지나쳤던 이유가 그것이다.
 * {@link ScheduledAnnotationBeanPostProcessor} 에게 <b>실제로 등록된 태스크 수</b>를 물으면
 * 그 수가 코드와 자동으로 같아진다.
 *
 * <p><b>잡보다 먼저 온다.</b> {@code JobLauncherApplicationRunner} 의 정렬값이 0 이라
 * 정렬값을 안 주면 그 뒤에 오는데, 그러면 잡이 먼저 뜨고 나서 가드가 말한다.
 * {@code SchemaPresenceGuard} 가 같은 이유로 같은 자리를 잡는다.
 */
@Component
@Order(SchedulerPoolGuard.ORDER)
public class SchedulerPoolGuard implements ApplicationRunner {

    /** {@code JobLauncherApplicationRunner}(0) 보다 앞. */
    static final int ORDER = -100;

    private static final Logger log = LoggerFactory.getLogger(SchedulerPoolGuard.class);

    private final ThreadPoolTaskScheduler taskScheduler;
    private final ScheduledAnnotationBeanPostProcessor scheduledTasks;

    public SchedulerPoolGuard(
            @Qualifier("taskScheduler") ThreadPoolTaskScheduler taskScheduler,
            ScheduledAnnotationBeanPostProcessor scheduledTasks) {
        this.taskScheduler = taskScheduler;
        this.scheduledTasks = scheduledTasks;
    }

    @Override
    public void run(ApplicationArguments args) {
        int registered = scheduledTasks.getScheduledTasks().size();
        int pool = taskScheduler.getScheduledThreadPoolExecutor().getCorePoolSize();

        if (pool < registered) {
            throw new IllegalStateException(
                    "spring.task.scheduling.pool.size 가 등록된 스케줄 작업 수보다 작습니다. "
                            + "풀=" + pool + " 등록=" + registered
                            + " (@Scheduled 애노테이션과 SchedulingConfigurer 로 등록된 것을 "
                            + "모두 셉니다 — 애노테이션만 grep 하면 수가 안 맞습니다). "
                            + "그러면 한 작업이 도는 동안 다른 작업이 스레드를 기다립니다 — "
                            + "검증은 한 번에 8분을 잡고 있고, 그동안 되읽기가 못 돌면 게이지가 "
                            + "그만큼 낡습니다. 배치는 멀쩡한데 SLA 알림이 울고, 되읽기가 "
                            + "실패한 것도 아니라 cy_batch_refresh_failures_total 도 안 오릅니다. "
                            + "BATCH_SCHEDULER_POOL_SIZE 를 " + registered + " 이상으로 주십시오.");
        }
        log.info("스케줄러 풀이 등록된 스케줄 작업 수를 받칩니다. 풀={} 등록={}", pool, registered);
    }
}

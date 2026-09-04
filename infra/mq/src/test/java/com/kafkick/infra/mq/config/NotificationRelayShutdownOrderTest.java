package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.kafkick.infra.mq.notification.NotificationOutboxRelay;
import com.kafkick.infra.mq.notification.NotificationRelayProperties;

/**
 * <b>릴레이가 워커 풀보다 먼저 멈춰야 한다.</b> 안 그러면 그 사이 스케줄러가 한 회차를
 * 도는데 풀은 이미 멈춰 있어 <b>제출이 거부되고 집어 둔 행이 붕 뜬다.</b>
 *
 * <h2>이 테스트의 첫 판은 엉뚱한 것을 재고 있었다</h2>
 *
 * <p>처음에는 {@code destroy()} 호출 순서만 봤고 "릴레이 → 풀" 이 나와서 통과했다.
 * 그런데 {@code ThreadPoolTaskExecutor} 는 {@code SmartLifecycle} 이기도 해서
 * <b>소멸보다 먼저 {@code stop()} 으로 멈춘다.</b> 실측한 전체 순서는 이랬다 —
 * {@code destroyMethod = "close"} 를 쓰던 시절:
 *
 * <pre>
 *   pool.stop(cb) → relay.close → pool.destroy → pool.shutdown
 * </pre>
 *
 * <p><b>풀이 먼저 멈추고 있었다.</b> 소멸 순서만 재던 테스트는 그것을 볼 수 없었다.
 * 리뷰가 짚었고, 그래서 릴레이를 {@code SmartLifecycle} 로 바꿨다 — 풀의 단계가
 * {@code Integer.MAX_VALUE / 2} 고 스프링은 <b>단계 내림차순</b>으로 멈추므로 기본 단계인
 * 릴레이가 먼저 간다. 지금 순서는 {@code relay.stop → pool.stop → …} 이다.
 *
 * <p>그래서 이 테스트는 <b>{@code stop} 까지 계측한다.</b> 안 그러면 같은 눈속임이 다시 통과한다.
 *
 * <p>진짜 릴레이 대신 <b>같은 모양의 두 빈</b>으로 잰다. 릴레이를 쓰려면 저장소와 발행기가
 * 필요한데, 여기서 보려는 것은 <b>스프링의 종료 순서</b>이지 릴레이의 동작이 아니다.
 */
class NotificationRelayShutdownOrderTest {

    static final List<String> EVENTS = new ArrayList<>();

    @Configuration(proxyBeanMethods = false)
    static class SameShapeAsRelayConfig {

        @Bean
        ThreadPoolTaskExecutor notificationRelayWorkers() {
            ThreadPoolTaskExecutor workers = new ThreadPoolTaskExecutor() {
                @Override
                public void stop(Runnable callback) {
                    EVENTS.add("pool.stop");
                    super.stop(callback);
                }

                @Override
                public void destroy() {
                    EVENTS.add("pool.destroy");
                    super.destroy();
                }
            };
            workers.setCorePoolSize(1);
            workers.setMaxPoolSize(1);
            workers.initialize();
            return workers;
        }

        /** 릴레이와 같은 자리 — 풀을 주입받고 {@link SmartLifecycle} 로 멈춘다. */
        @Bean
        SmartLifecycle relayLike(ThreadPoolTaskExecutor notificationRelayWorkers) {
            return new SmartLifecycle() {
                private volatile boolean running;

                @Override
                public void start() {
                    running = true;
                }

                @Override
                public void stop() {
                    EVENTS.add("relay.stop");
                    running = false;
                }

                @Override
                public boolean isRunning() {
                    return running;
                }
            };
        }
    }

    @Test
    @DisplayName("릴레이가 워커 풀보다 먼저 멈춘다 — 소멸이 아니라 lifecycle stop 을 본다")
    void theRelayStopsBeforeThePool() {
        EVENTS.clear();
        try (var ctx = new AnnotationConfigApplicationContext(SameShapeAsRelayConfig.class)) {
            assertThat(ctx.getBean(ThreadPoolTaskExecutor.class)).isNotNull();
        }
        assertThat(EVENTS)
                .as("풀이 먼저 멈추면 그 사이 회차의 제출이 거부되고 집어 둔 행이 붕 뜹니다")
                .containsSubsequence("relay.stop", "pool.stop")
                .containsSubsequence("pool.stop", "pool.destroy");
    }

    /**
     * <b>진짜 릴레이가 그 자리에 설 수 있어야 한다.</b> 위 테스트는 모양만 같은 대역으로
     * 순서를 재므로, 릴레이가 실제로 {@link SmartLifecycle} 이 아니게 되면 <b>대역만
     * 통과하고 배선은 조용히 틀린다.</b>
     */
    @Test
    @DisplayName("릴레이가 SmartLifecycle 이다 — 대역이 아니라 진짜 타입을 본다")
    void theRelayItselfIsALifecycleBean() {
        assertThat(SmartLifecycle.class)
                .as("SmartLifecycle 이 아니면 소멸 콜백으로 밀려 풀보다 늦게 멈춥니다")
                .isAssignableFrom(NotificationOutboxRelay.class);
    }

    /**
     * <b>풀 크기가 릴레이의 lease 계산과 갈리면 안 된다.</b> 릴레이 생성자는
     * {@code ceil(maxInFlight / workerCount)} 를 파도 깊이로 잡고 lease 와 견주는데,
     * 실제 풀이 그보다 작으면 <b>더 깊은 파도가 돌고 검사는 통과한다</b> — 뒤쪽 행이
     * 처리 전에 회수되어 중복 발행이 된다. 증상이 조용한 쪽이다.
     *
     * <p>그래서 설정이 둘 다 같은 속성에서 꺼내는지를 여기서 못 박는다. 큐 용량도 같이
     * 본다 — 무한 큐면 백프레셔를 걷어낸 실수가 거부가 아니라 <b>OOM</b> 으로 나타난다.
     */
    @Test
    @DisplayName("워커 풀 크기와 큐 용량이 릴레이가 쓰는 그 속성에서 나온다")
    void thePoolIsSizedFromTheSamePropertiesTheLeaseCheckUses() {
        NotificationRelayProperties properties = new NotificationRelayProperties();
        properties.setWorkerCount(3);
        properties.setMaxInFlight(9);

        ThreadPoolTaskExecutor workers =
                new NotificationRelayConfig().notificationRelayWorkers(properties);
        try {
            assertThat(workers.getCorePoolSize()).isEqualTo(3);
            assertThat(workers.getMaxPoolSize())
                    .as("최대가 코어보다 크면 실제 동시성이 파도 계산보다 깊어집니다")
                    .isEqualTo(3);
            assertThat(workers.getThreadPoolExecutor().getQueue().remainingCapacity())
                    .as("무한 큐면 백프레셔를 걷어낸 실수가 거부가 아니라 OOM 으로 납니다")
                    .isEqualTo(9);
        } finally {
            workers.destroy();
        }
    }
}

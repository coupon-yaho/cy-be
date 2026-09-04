package com.kafkick.infra.mq.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.kafkick.infra.mq.notification.NotificationRelayProperties;

/**
 * <b>종료 순서는 재 봐야 아는 것이다.</b>
 *
 * <p>{@link NotificationRelayConfig} 는 릴레이가 <b>풀보다 먼저</b> 소멸되는 것에 기대고 있다 —
 * 릴레이의 {@code close()} 가 인플라이트를 배수하는데, 풀이 먼저 닫히면 배수할 작업이
 * 중단되거나 영영 안 끝난다. 그래서 풀 쪽은 {@code waitForTasksToCompleteOnShutdown} 을
 * 끄고 있는데, <b>그 선택이 안전한 이유가 전적으로 이 순서다.</b>
 *
 * <p>스프링은 소멸을 의존 역순으로 한다고 알려져 있지만, 그것을 <b>주석에 단정으로 적는 대신</b>
 * 여기서 잰다. 순서가 뒤집히면 증상은 조용하다 — 종료 때 {@code IN_PROGRESS} 가 남고,
 * 그 행은 다음 기동 뒤 lease 만료까지 아무도 못 집는다.
 *
 * <p>진짜 릴레이 대신 <b>같은 모양의 두 빈</b>으로 잰다. 릴레이를 쓰려면 저장소와 발행기가
 * 필요한데, 여기서 보려는 것은 <b>스프링의 소멸 순서</b>이지 릴레이의 동작이 아니다.
 */
class NotificationRelayShutdownOrderTest {

    static final List<String> DESTROYED = new ArrayList<>();

    @Configuration(proxyBeanMethods = false)
    static class SameShapeAsRelayConfig {

        @Bean
        ThreadPoolTaskExecutor notificationRelayWorkers() {
            ThreadPoolTaskExecutor workers = new ThreadPoolTaskExecutor() {
                @Override
                public void destroy() {
                    DESTROYED.add("pool");
                    super.destroy();
                }
            };
            workers.setCorePoolSize(1);
            workers.setMaxPoolSize(1);
            workers.initialize();
            return workers;
        }

        /** 릴레이와 같은 자리 — 풀을 주입받고 {@code close} 로 소멸된다. */
        @Bean(destroyMethod = "close")
        AutoCloseable relayLike(ThreadPoolTaskExecutor notificationRelayWorkers) {
            return () -> DESTROYED.add("relay");
        }
    }

    @Test
    @DisplayName("릴레이가 워커 풀보다 먼저 소멸된다 — 배수가 풀보다 늦으면 인플라이트가 잘린다")
    void theRelayIsDestroyedBeforeThePool() {
        DESTROYED.clear();
        try (var ctx = new AnnotationConfigApplicationContext(SameShapeAsRelayConfig.class)) {
            assertThat(ctx.getBean(ThreadPoolTaskExecutor.class)).isNotNull();
        }
        assertThat(DESTROYED)
                .as("풀이 먼저 닫히면 릴레이의 배수가 중단된 작업을 기다리게 됩니다")
                .containsExactly("relay", "pool");
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

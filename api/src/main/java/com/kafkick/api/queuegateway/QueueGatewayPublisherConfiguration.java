package com.kafkick.api.queuegateway;

import java.time.Clock;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.stock.AdminStockResolver;
import com.kafkick.core.admin.stock.V2AdminStockReader;
import com.kafkick.core.queuegateway.QueueGatewayStatePort;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;

/** 명시적으로 활성화한 API에만 외부 게이트웨이 상태 공급 스케줄을 조립합니다. */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@EnableConfigurationProperties(QueueGatewayPublisherProperties.class)
@ConditionalOnProperty(prefix = "queue.gateway.publisher", name = "enabled", havingValue = "true")
public class QueueGatewayPublisherConfiguration {

    /** 종료 때 도는 보고를 기다리는 상한. 1초 주기 작업이라 이보다 오래 잡을 이유가 없다. */
    private static final int SHUTDOWN_AWAIT_SECONDS = 3;

    static final String CAPACITY_SCHEDULER = "queueGatewayCapacityScheduler";
    static final String COUPON_ROUND_SCHEDULER = "queueGatewayCouponRoundScheduler";

    /**
     * <b>{@code @DependsOn} 은 기동 순서가 아니라 <i>종료</i> 순서 때문에 있다.</b>
     *
     * <p>스프링은 의존하는 빈을 의존 대상보다 <b>먼저</b> 파괴한다. 그래서 스케줄러가
     * 공급기에 의존하게 두면 <b>스케줄러가 먼저 멈추고 그다음에 공급기의 제거가 돈다.</b>
     * 반대로 두면 제거가 끝난 뒤 늦게 끝난 보고가 필드를 되살려, 게이트웨이가 이미 내려간
     * 인스턴스의 몫을 계속 합산해 죽은 서버로 사람을 들여보낸다.
     */
    @Bean(name = CAPACITY_SCHEDULER)
    @DependsOn("queueGatewayCapacityPublisher")
    ThreadPoolTaskScheduler queueGatewayCapacityScheduler() {
        return scheduler("queue-gateway-capacity-");
    }

    /** 위와 같은 이유로 공급기에 의존시킨다 — 종료 때 이 스케줄러가 먼저 멈춰야 한다. */
    @Bean(name = COUPON_ROUND_SCHEDULER)
    @DependsOn("queueGatewayCouponRoundPublisher")
    ThreadPoolTaskScheduler queueGatewayCouponRoundScheduler() {
        return scheduler("queue-gateway-coupon-round-");
    }

    @Bean
    QueueGatewayCapacityPublisher queueGatewayCapacityPublisher(
            QueueGatewayStatePort statePort,
            ApplicationAvailability availability,
            QueueGatewayPublisherProperties properties,
            ObjectProvider<Clock> clock
    ) {
        return new QueueGatewayCapacityPublisher(
                statePort, availability, properties, clock.getIfAvailable(Clock::systemUTC));
    }

    @Bean
    QueueGatewayCouponRoundPublisher queueGatewayCouponRoundPublisher(
            AdminCouponRoundDataReader couponRoundDataReader,
            ObjectProvider<V2AdminStockReader> v2StockReader,
            RuntimeConfigStore runtimeConfigStore,
            QueueGatewayStatePort statePort,
            ObjectProvider<Clock> clock
    ) {
        return new QueueGatewayCouponRoundPublisher(
                couponRoundDataReader,
                new AdminStockResolver(v2StockReader.getIfAvailable(AdminStockResolver::unavailableV2Reader)),
                runtimeConfigStore,
                statePort,
                clock.getIfAvailable(Clock::systemUTC));
    }

    /**
     * <b>도는 보고가 끝날 때까지 기다린다.</b> 예전에는 안 기다렸는데
     * ({@code waitForTasksToCompleteOnShutdown=false}), 그러면 종료가 진행 중인 작업을
     * 인터럽트만 하고 지나간다. Redis 왕복이 인터럽트에 즉시 반응하지 않으면 <b>제거가 끝난
     * 뒤에 쓰기가 완료</b>되어 필드가 되살아난다.
     *
     * <p>기다리는 시간을 짧게 묶는 이유 — 이 보고는 1초 주기이고 실패해도 게이트웨이가
     * 하한으로 물러난다. 종료를 몇 초씩 잡을 값어치가 없다. 그 안에 안 끝나면
     * 인터럽트로 넘어가는데, 그때는 위 {@code @DependsOn} 이 만든 순서가 남는다.
     */
    private static ThreadPoolTaskScheduler scheduler(String threadNamePrefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix(threadNamePrefix);
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(SHUTDOWN_AWAIT_SECONDS);
        return scheduler;
    }
}

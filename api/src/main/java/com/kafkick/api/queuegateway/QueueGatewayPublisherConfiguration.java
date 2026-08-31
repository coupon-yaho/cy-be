package com.kafkick.api.queuegateway;

import java.time.Clock;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    static final String CAPACITY_SCHEDULER = "queueGatewayCapacityScheduler";
    static final String COUPON_ROUND_SCHEDULER = "queueGatewayCouponRoundScheduler";

    @Bean(name = CAPACITY_SCHEDULER)
    ThreadPoolTaskScheduler queueGatewayCapacityScheduler() {
        return scheduler("queue-gateway-capacity-");
    }

    @Bean(name = COUPON_ROUND_SCHEDULER)
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

    private static ThreadPoolTaskScheduler scheduler(String threadNamePrefix) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix(threadNamePrefix);
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        return scheduler;
    }
}

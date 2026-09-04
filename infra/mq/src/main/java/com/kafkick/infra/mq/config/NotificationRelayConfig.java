package com.kafkick.infra.mq.config;

import java.time.Clock;

import javax.sql.DataSource;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;
import com.kafkick.infra.mq.notification.FullJitterBackOff;
import com.kafkick.infra.mq.notification.NotificationOutboxRelay;
import com.kafkick.infra.mq.notification.NotificationRelayProperties;
import com.kafkick.infra.mq.notification.NotificationRelayScheduler;
import com.kafkick.infra.mq.notification.RelayBinlogFormatGuard;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty("kafka.enabled")
@EnableScheduling
@EnableConfigurationProperties(NotificationRelayProperties.class)
public class NotificationRelayConfig {
    @Bean
    public NotificationOutboxRelay notificationOutboxRelay(
            NotificationOutboxRepository outboxes,
            NotificationRepository notifications,
            NotificationRequestedEventPublisher publisher,
            NotificationRelayProperties properties,
            ObjectProvider<Clock> clocks) {
        return new NotificationOutboxRelay(outboxes, notifications, publisher,
                properties.getLease(),
                properties.getClaimBatchSize(),
                new FullJitterBackOff(properties.getBackoffBase(), properties.getBackoffCap()),
                clocks.getIfAvailable(Clock::systemUTC));
    }

    @Bean
    public NotificationRelayScheduler notificationRelayScheduler(NotificationOutboxRelay relay) {
        return new NotificationRelayScheduler(relay);
    }

    /**
     * <b>릴레이가 도는 곳에 둔다.</b> 같은 검사가 {@code batch} 모듈에 있지만 이 릴레이는
     * {@code api} 애플리케이션에서 돌고, {@code api} 는 그 모듈을 의존하지 않는다.
     */
    @Bean
    public RelayBinlogFormatGuard relayBinlogFormatGuard(DataSource dataSource) {
        return new RelayBinlogFormatGuard(dataSource);
    }
}

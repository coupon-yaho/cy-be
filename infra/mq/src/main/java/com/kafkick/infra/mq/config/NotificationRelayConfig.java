package com.kafkick.infra.mq.config;

import java.time.Clock;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.event.NotificationRequestedEventPublisher;
import com.kafkick.infra.mq.notification.NotificationOutboxRelay;
import com.kafkick.infra.mq.notification.NotificationRelayProperties;
import com.kafkick.infra.mq.notification.NotificationRelayScheduler;

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
                properties.getLease(), clocks.getIfAvailable(Clock::systemUTC));
    }

    @Bean
    public NotificationRelayScheduler notificationRelayScheduler(NotificationOutboxRelay relay) {
        return new NotificationRelayScheduler(relay);
    }
}

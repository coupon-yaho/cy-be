package com.kafkick.api.admin.notification;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.notification.NotificationQueryService;
import com.kafkick.core.notification.NotificationRepository;
import com.kafkick.core.notification.NotificationOutboxRepository;
import com.kafkick.core.notification.NotificationRejectedAuditWriter;
import com.kafkick.core.notification.NotificationResendAuditRepository;
import com.kafkick.core.notification.NotificationResendService;
import com.kafkick.core.support.TimeProvider;

@Configuration(proxyBeanMethods = false)
public class AdminNotificationConfig {
    @Bean
    @ConditionalOnMissingBean(NotificationQueryService.class)
    public NotificationQueryService notificationQueryService(
            NotificationRepository notifications,
            CouponRoundRepository couponRounds,
            TimeProvider timeProvider) {
        return new NotificationQueryService(notifications, couponRounds, timeProvider);
    }

    @Bean
    @ConditionalOnMissingBean(NotificationResendService.class)
    public NotificationResendService notificationResendService(
            NotificationRepository notifications,
            NotificationOutboxRepository outboxes,
            NotificationResendAuditRepository audits,
            NotificationRejectedAuditWriter rejectedAudits,
            TimeProvider timeProvider) {
        return new NotificationResendService(notifications, outboxes, audits,
                rejectedAudits, timeProvider);
    }

}

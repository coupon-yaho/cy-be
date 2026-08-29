package com.kafkick.api.admin.notification;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafkick.core.coupon.port.CouponRoundRepository;
import com.kafkick.core.notification.NotificationQueryService;
import com.kafkick.core.notification.NotificationRepository;
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

}

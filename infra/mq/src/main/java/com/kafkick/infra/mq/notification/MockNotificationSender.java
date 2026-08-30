package com.kafkick.infra.mq.notification;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.kafkick.core.notification.NotificationSender;
import com.kafkick.core.notification.domain.Notification;

@Component
@ConditionalOnProperty("kafka.enabled")
public class MockNotificationSender implements NotificationSender {
    @Override
    public void send(Notification notification) {
        if (notification == null) {
            throw new IllegalArgumentException("notification은 필수입니다.");
        }
    }
}

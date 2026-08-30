package com.kafkick.core.notification.event;

public interface NotificationRequestedEventPublisher {
    void publish(NotificationRequestedEvent event);
}

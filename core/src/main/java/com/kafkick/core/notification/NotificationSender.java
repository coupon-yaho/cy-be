package com.kafkick.core.notification;

import com.kafkick.core.notification.domain.Notification;

public interface NotificationSender {
    void send(Notification notification);
}

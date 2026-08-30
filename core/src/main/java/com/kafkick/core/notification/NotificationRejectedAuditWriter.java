package com.kafkick.core.notification;

import java.time.Instant;

public interface NotificationRejectedAuditWriter {
    void write(Long notificationId, Long requestedBy, Instant requestedAt, String rejectCode);
}

package com.kafkick.storage.db.notification.repository;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.notification.NotificationRejectedAuditWriter;
import com.kafkick.core.notification.NotificationResendAuditRepository;
import com.kafkick.core.notification.domain.NotificationResendAudit;

@Component
public class TransactionalNotificationRejectedAuditWriter
        implements NotificationRejectedAuditWriter {
    private final NotificationResendAuditRepository audits;

    public TransactionalNotificationRejectedAuditWriter(NotificationResendAuditRepository audits) {
        this.audits = Objects.requireNonNull(audits, "audits");
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void write(Long notificationId, Long requestedBy, Instant requestedAt,
            String rejectCode) {
        audits.save(new NotificationResendAudit(null, notificationId, null, requestedBy,
                requestedAt, false, rejectCode, requestedAt));
    }
}

package com.kafkick.core.notification;

import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutbox;

@Service
public class NotificationRequestService {
    private static final int INITIAL_ATTEMPT_SEQUENCE = 1;

    private final NotificationRepository notifications;
    private final NotificationOutboxRepository outboxes;
    private final NotificationPayloadFactory payloads;

    public NotificationRequestService(NotificationRepository notifications,
            NotificationOutboxRepository outboxes, NotificationPayloadFactory payloads) {
        this.notifications = Objects.requireNonNull(notifications, "notifications");
        this.outboxes = Objects.requireNonNull(outboxes, "outboxes");
        this.payloads = Objects.requireNonNull(payloads, "payloads");
    }

    public Notification request(Issuance issuance) {
        Objects.requireNonNull(issuance, "issuance");
        if (issuance.id() == null) {
            throw new IllegalArgumentException("저장된 발급건만 알림을 요청할 수 있습니다.");
        }
        NotificationPayload payload = payloads.create(issuance);
        Notification notification = notifications.save(Notification.pending(
                issuance.couponRoundId(), issuance.memberId(), issuance.id(),
                payload.recipientContact(), payload.messageBody(), issuance.issuedAt()));
        outboxes.save(NotificationOutbox.pending(notification.id(), INITIAL_ATTEMPT_SEQUENCE,
                AttemptTrigger.INITIAL, issuance.issuedAt()));
        return notification;
    }
}

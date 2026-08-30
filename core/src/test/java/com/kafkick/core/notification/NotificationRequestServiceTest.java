package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.membership.domain.MembershipGrade;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationStatus;

@ExtendWith(MockitoExtension.class)
class NotificationRequestServiceTest {
    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");

    @Mock NotificationRepository notifications;
    @Mock NotificationOutboxRepository outboxes;

    @Test
    void savesPendingNotificationAndInitialOutbox() {
        NotificationPayloadFactory payloads = issuance -> new NotificationPayload(
                "member:" + issuance.memberId(), "coupon-issued:" + issuance.id());
        NotificationRequestService service = new NotificationRequestService(
                notifications, outboxes, payloads);
        when(notifications.save(any(Notification.class))).thenAnswer(invocation -> {
            Notification pending = invocation.getArgument(0);
            return new Notification(41L, pending.couponId(), pending.memberId(),
                    pending.issuanceId(), pending.channel(), pending.status(),
                    pending.attemptCount(), pending.resendCount(), pending.lastFailureReason(),
                    pending.recipientContact(), pending.messageBody(), pending.createdAt(),
                    pending.updatedAt(), pending.sentAt(), pending.failedAt());
        });
        when(outboxes.save(any(NotificationOutbox.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Notification saved = service.request(savedIssuance());

        assertThat(saved.status()).isEqualTo(NotificationStatus.PENDING);
        assertThat(saved.attemptCount()).isZero();
        assertThat(saved.recipientContact()).isEqualTo("member:20");
        assertThat(saved.messageBody()).isEqualTo("coupon-issued:100");
        ArgumentCaptor<NotificationOutbox> outbox = ArgumentCaptor.forClass(NotificationOutbox.class);
        verify(outboxes).save(outbox.capture());
        assertThat(outbox.getValue().notificationId()).isEqualTo(41L);
        assertThat(outbox.getValue().attemptSeq()).isEqualTo(1);
        assertThat(outbox.getValue().trigger()).isEqualTo(AttemptTrigger.INITIAL);
        assertThat(outbox.getValue().createdAt()).isEqualTo(AT);
    }

    private static Issuance savedIssuance() {
        return Issuance.restore(100L, 10L, 20L, "ABCDEFGHJKLM2345", MembershipGrade.GOLD,
                IssuanceStatus.ISSUED, AT, AT.plusSeconds(86_400), AT);
    }
}

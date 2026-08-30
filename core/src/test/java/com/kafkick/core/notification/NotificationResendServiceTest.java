package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationOutbox;
import com.kafkick.core.notification.domain.NotificationResendAudit;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

@ExtendWith(MockitoExtension.class)
class NotificationResendServiceTest {
    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");
    private static final long ADMIN_ID = 99L;

    @Mock NotificationRepository notifications;
    @Mock NotificationOutboxRepository outboxes;
    @Mock NotificationResendAuditRepository audits;
    @Mock NotificationRejectedAuditWriter rejectedAudits;
    private NotificationResendService service;

    @BeforeEach
    void setUp() {
        service = new NotificationResendService(notifications, outboxes, audits, rejectedAudits,
                new TimeProvider(Clock.fixed(AT, ZoneOffset.UTC)));
    }

    @Test
    void missingNotificationIsAuditedAndRejected() {
        when(notifications.findById(41L)).thenReturn(Optional.empty());

        assertRejection(NotificationResendRejection.NOT_FOUND, "NOTIFY-006",
                () -> service.resend(41L, ADMIN_ID));

        verify(rejectedAudits).write(41L, ADMIN_ID, AT, "ADMIN-005");
    }

    @Test
    void nonFailedStatusIsConflict() {
        when(notifications.findById(41L)).thenReturn(Optional.of(notification(
                NotificationStatus.SENT, 1, 0)));

        assertRejection(NotificationResendRejection.CONFLICT, "NOTIFY-007",
                () -> service.resend(41L, ADMIN_ID));
        verify(rejectedAudits).write(41L, ADMIN_ID, AT, "ADMIN-006");
    }

    @Test
    void resendLimitIsAuditedBeforeWindowCheck() {
        when(notifications.findById(41L)).thenReturn(Optional.of(notification(
                NotificationStatus.DEAD, 5, 3)));

        assertRejection(NotificationResendRejection.LIMIT_EXCEEDED, "NOTIFY-002",
                () -> service.resend(41L, ADMIN_ID));
        verify(rejectedAudits).write(41L, ADMIN_ID, AT, "ADMIN-007");
    }

    @Test
    void acceptedAuditInsideTenMinutesIsConflict() {
        when(notifications.findById(41L)).thenReturn(Optional.of(notification(
                NotificationStatus.FAILED, 1, 0)));
        when(audits.findLatestAcceptedByNotificationId(41L)).thenReturn(Optional.of(
                new NotificationResendAudit(7L, 41L, 2, ADMIN_ID, AT.minusSeconds(599),
                        true, null, AT.minusSeconds(599))));

        assertRejection(NotificationResendRejection.CONFLICT, "NOTIFY-007",
                () -> service.resend(41L, ADMIN_ID));
        verify(rejectedAudits).write(41L, ADMIN_ID, AT, "ADMIN-006");
    }

    @Test
    void casLossIsConflict() {
        Notification failed = notification(NotificationStatus.FAILED, 1, 0);
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(audits.findLatestAcceptedByNotificationId(41L)).thenReturn(Optional.empty());
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class)))
                .thenReturn(false);

        assertRejection(NotificationResendRejection.CONFLICT, "NOTIFY-007",
                () -> service.resend(41L, ADMIN_ID));
        verify(rejectedAudits).write(41L, ADMIN_ID, AT, "ADMIN-006");
    }

    @Test
    void acceptedRequestClaimsThreeAxesAndStoresAuditAndManualOutbox() {
        Notification failed = notification(NotificationStatus.FAILED, 1, 0);
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(audits.findLatestAcceptedByNotificationId(41L)).thenReturn(Optional.empty());
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class)))
                .thenReturn(true);

        NotificationResendResult result = service.resend(41L, ADMIN_ID);

        assertThat(result.notificationId()).isEqualTo(41L);
        assertThat(result.attemptSeq()).isEqualTo(2);
        assertThat(result.requestedAt()).isEqualTo(AT);
        ArgumentCaptor<Notification> claimed = ArgumentCaptor.forClass(Notification.class);
        verify(notifications).saveIfStatus(claimed.capture(),
                org.mockito.ArgumentMatchers.eq(NotificationStatus.FAILED),
                org.mockito.ArgumentMatchers.eq(1), org.mockito.ArgumentMatchers.eq(0));
        assertThat(claimed.getValue().status()).isEqualTo(NotificationStatus.SENDING);
        assertThat(claimed.getValue().resendCount()).isEqualTo(1);
        ArgumentCaptor<NotificationResendAudit> audit =
                ArgumentCaptor.forClass(NotificationResendAudit.class);
        verify(audits).save(audit.capture());
        assertThat(audit.getValue().accepted()).isTrue();
        assertThat(audit.getValue().attemptSeq()).isEqualTo(2);
        ArgumentCaptor<NotificationOutbox> outbox = ArgumentCaptor.forClass(NotificationOutbox.class);
        verify(outboxes).save(outbox.capture());
        assertThat(outbox.getValue().trigger()).isEqualTo(AttemptTrigger.MANUAL);
        assertThat(outbox.getValue().attemptSeq()).isEqualTo(2);
    }

    private void assertRejection(NotificationResendRejection rejection, String expectedCode,
            org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action)
                .isInstanceOfSatisfying(NotificationResendRejectedException.class,
                        exception -> {
                            assertThat(exception.rejection()).isEqualTo(rejection);
                            assertThat(exception).isInstanceOfSatisfying(BusinessException.class,
                                    business -> assertThat(business.getErrorCode().getCode())
                                            .isEqualTo(expectedCode));
                        });
    }

    private static Notification notification(NotificationStatus status, int attempts, int resends) {
        boolean sent = status == NotificationStatus.SENT;
        boolean failed = status == NotificationStatus.FAILED || status == NotificationStatus.DEAD;
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                status, attempts, resends,
                failed ? NotifyFailureReason.SEND_TIMEOUT : null,
                "member:20", "coupon-issued:100", AT, AT,
                sent ? AT : null, failed ? AT : null);
    }
}

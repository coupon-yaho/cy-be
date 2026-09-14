package com.kafkick.core.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.notification.NotificationDeliveryDecision.Action;
import com.kafkick.core.notification.domain.AttemptResult;
import com.kafkick.core.notification.domain.AttemptTrigger;
import com.kafkick.core.notification.domain.Notification;
import com.kafkick.core.notification.domain.NotificationAttempt;
import com.kafkick.core.notification.domain.NotificationStatus;
import com.kafkick.core.notification.domain.NotifyFailureReason;
import com.kafkick.core.notification.event.NotificationRequestedEvent;

@ExtendWith(MockitoExtension.class)
class NotificationDeliveryServiceTest {
    private static final Instant AT = Instant.parse("2026-08-29T00:00:00Z");

    @Mock NotificationRepository notifications;
    @Mock NotificationAttemptRepository attempts;
    @Mock NotificationOutboxRepository outboxes;
    private NotificationDeliveryService service;

    @BeforeEach
    void setUp() {
        service = new NotificationDeliveryService(notifications, attempts, outboxes);
    }

    @Test
    void preparesInitialPendingAttemptWithCas() {
        Notification pending = pending();
        when(notifications.findById(41L)).thenReturn(Optional.of(pending));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of());
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class))).thenReturn(true);

        NotificationDeliveryDecision decision = service.prepare(event(1, AttemptTrigger.INITIAL), AT);

        assertThat(decision.action()).isEqualTo(Action.SEND);
        assertThat(decision.attemptSeq()).isEqualTo(1);
        assertThat(decision.trigger()).isEqualTo(AttemptTrigger.INITIAL);
        assertThat(decision.notification().status()).isEqualTo(NotificationStatus.SENDING);
    }

    @Test
    void continuesRetryableFailureAsAutoAttempt() {
        Notification failed = pending().startSending(AttemptTrigger.INITIAL, AT)
                .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(1));
        NotificationAttempt first = failedAttempt(1, AttemptTrigger.INITIAL);
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(first));
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class))).thenReturn(true);

        NotificationDeliveryDecision decision = service.prepare(event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.SEND);
        assertThat(decision.attemptSeq()).isEqualTo(2);
        assertThat(decision.trigger()).isEqualTo(AttemptTrigger.AUTO);
    }

    @Test
    void rejectsAnOlderCommandAfterManualLineageStarted() {
        Notification failed = new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.FAILED, 2, 1, NotifyFailureReason.SEND_TIMEOUT,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(
                failedAttempt(1, AttemptTrigger.INITIAL), failedAttempt(2, AttemptTrigger.MANUAL)));

        NotificationDeliveryDecision decision = service.prepare(event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.ACKNOWLEDGE);
    }

    @Test
    void delayedInitialEventCannotResumeInFlightManualAttempt() {
        Notification manualSending = new Notification(41L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.SENDING, 5, 1, null,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(manualSending));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(
                failedAttempt(1, AttemptTrigger.INITIAL),
                failedAttempt(2, AttemptTrigger.AUTO),
                failedAttempt(3, AttemptTrigger.AUTO),
                failedAttempt(4, AttemptTrigger.AUTO)));
        when(outboxes.findTriggerByNotificationIdAndAttemptSeq(41L, 5))
                .thenReturn(Optional.of(AttemptTrigger.MANUAL));

        NotificationDeliveryDecision decision = service.prepare(
                event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.ACKNOWLEDGE);
    }

    @Test
    void delayedRefundedManualEventCannotResumeNextManualAttempt() {
        Notification manualSending = new Notification(41L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.SENDING, 6, 1, null,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(manualSending));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(
                failedAttempt(1, AttemptTrigger.INITIAL),
                failedAttempt(2, AttemptTrigger.AUTO),
                failedAttempt(3, AttemptTrigger.AUTO),
                failedAttempt(4, AttemptTrigger.AUTO),
                failedAttempt(5, AttemptTrigger.MANUAL)));
        when(outboxes.findTriggerByNotificationIdAndAttemptSeq(41L, 6))
                .thenReturn(Optional.of(AttemptTrigger.MANUAL));

        NotificationDeliveryDecision decision = service.prepare(
                event(5, AttemptTrigger.MANUAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.ACKNOWLEDGE);
    }

    @Test
    void completedCurrentAttemptIsAcknowledgedWithoutAnotherSenderCall() {
        Notification sending = pending().startSending(AttemptTrigger.INITIAL, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(sending));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of(
                failedAttempt(1, AttemptTrigger.INITIAL)));

        NotificationDeliveryDecision decision = service.prepare(
                event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(decision.action()).isEqualTo(Action.ACKNOWLEDGE);
    }

    @Test
    void completedAttemptInsertWinnerAloneMarksSuccess() {
        Notification sending = pending().startSending(AttemptTrigger.INITIAL, AT);
        NotificationDeliveryDecision decision = NotificationDeliveryDecision.send(sending, 1,
                1, AttemptTrigger.INITIAL);
        when(attempts.saveIfAbsent(any())).thenReturn(true);
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class))).thenReturn(true);

        boolean won = service.completeSuccess(decision, AT, AT.plusSeconds(1));

        assertThat(won).isTrue();
        ArgumentCaptor<Notification> next = ArgumentCaptor.forClass(Notification.class);
        verify(notifications).saveIfStatus(next.capture(),
                org.mockito.ArgumentMatchers.eq(NotificationStatus.SENDING),
                org.mockito.ArgumentMatchers.eq(1), org.mockito.ArgumentMatchers.eq(0));
        assertThat(next.getValue().status()).isEqualTo(NotificationStatus.SENT);
    }

    @Test
    void fourthFailureIsTerminalAndEarlierFailureRetries() {
        Notification firstSending = pending().startSending(AttemptTrigger.INITIAL, AT);
        when(attempts.saveIfAbsent(any())).thenReturn(true);
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class))).thenReturn(true);

        FailureOutcome first = service.completeFailure(
                NotificationDeliveryDecision.send(firstSending, 1, 1, AttemptTrigger.INITIAL),
                NotifyFailureReason.SEND_TIMEOUT, AT, AT.plusSeconds(1));
        Notification fourthSending = new Notification(41L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.SENDING, 4, 0, null,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        FailureOutcome fourth = service.completeFailure(
                NotificationDeliveryDecision.send(fourthSending, 1, 4, AttemptTrigger.AUTO),
                NotifyFailureReason.SEND_TIMEOUT, AT, AT.plusSeconds(1));

        assertThat(first).isEqualTo(FailureOutcome.RETRY);
        assertThat(fourth).isEqualTo(FailureOutcome.TERMINAL);
    }

    /**
     * 자동 재시도는 <b>같은 외부 키</b>로 나간다.
     *
     * <p>{@link NotificationSender} 가 산문으로 약속한 것이다 — <i>"같은 논리적 발송을
     * 가리키는 키. 자동 재시도 사이에 안 변한다"</i>. 그 문장을 지키는 시험이 없었다.
     *
     * <p>키는 {@code notification.id() + ":" + baseAttemptSeq} 이고 {@code baseAttemptSeq}
     * 는 <b>원래 이벤트</b>의 {@code attemptSeq} 다. 자동 재시도는 같은 이벤트를 다시
     * 소비하므로 그 값이 안 변한다 — {@code attemptCount} 가 2로 올라도 키는 그대로다.
     */
    @Test
    void keepsTheSameExternalKeyAcrossAutomaticRetries() {
        Notification pending = pending();
        when(notifications.findById(41L)).thenReturn(Optional.of(pending));
        when(attempts.findByNotificationId(41L)).thenReturn(List.of());
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class)))
                .thenReturn(true);
        String first = service.prepare(event(1, AttemptTrigger.INITIAL), AT).idempotencyKey();

        Notification failed = pending.startSending(AttemptTrigger.INITIAL, AT)
                .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(1));
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(attempts.findByNotificationId(41L))
                .thenReturn(List.of(failedAttempt(1, AttemptTrigger.INITIAL)));
        NotificationDeliveryDecision retry =
                service.prepare(event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(retry.action()).isEqualTo(Action.SEND);
        assertThat(retry.attemptSeq()).isEqualTo(2);          // 시도 번호는 는다
        assertThat(retry.idempotencyKey()).isEqualTo(first);  // **키는 안 변한다**
        assertThat(retry.idempotencyKey()).isEqualTo("41:1");
    }

    /**
     * 내용도 같이 유지된다. 요구사항 §5.1 은 <b>"외부 키와 신청 내용"</b> 둘을 함께 적는다.
     *
     * <p>⚠️ {@link Notification} 은 {@code (id, couponId, memberId, issuanceId, …)} 순이고
     * {@link NotificationRequestedEvent} 는 {@code (notificationId, memberId, couponId, …)}
     * 순이다 — <b>둘의 순서가 서로 다르다.</b> 자리만 보고 단언을 쓰다 한 번 틀렸다.
     */
    @Test
    void keepsTheSameContentAcrossAutomaticRetries() {
        Notification failed = pending().startSending(AttemptTrigger.INITIAL, AT)
                .markFailed(NotifyFailureReason.SEND_TIMEOUT, AT.plusSeconds(1));
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(attempts.findByNotificationId(41L))
                .thenReturn(List.of(failedAttempt(1, AttemptTrigger.INITIAL)));
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class)))
                .thenReturn(true);

        Notification sent = service.prepare(event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2))
                .notification();

        assertThat(sent.memberId()).isEqualTo(20L);
        assertThat(sent.couponId()).isEqualTo(10L);
        assertThat(sent.issuanceId()).isEqualTo(100L);
    }

    /**
     * ⚠️ <b>{@code MANUAL} 은 키가 바뀐다. 그리고 그것이 맞다.</b>
     *
     * <p>수동 재발송은 <b>일부러 한 번 더 보내는 것</b>이라 새 논리적 발송이다. 키를
     * 고정하면 외부가 그것을 앞 발송과 같은 것으로 보고 <b>버린다</b> — 운영자가 누른
     * 재발송이 조용히 사라진다.
     *
     * <p><b>옮길 때 조심할 자리다.</b> 사전예약의 <i>"승인된 재처리"</i>(요구사항 §5.1)는
     * 같은 예약을 다시 등록하는 것이라 <b>키가 바뀌면 외부에 둘이 생긴다</b> —
     * 같은 메커니즘이 두 도메인에서 반대 뜻이다. 이 시험은 <b>알림의</b> 규칙을 적는다.
     */
    @Test
    void manualReprocessDeliberatelyUsesANewExternalKey() {
        Notification failed = new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.FAILED, 1, 0, NotifyFailureReason.SEND_TIMEOUT,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(failed));
        when(attempts.findByNotificationId(41L))
                .thenReturn(List.of(failedAttempt(1, AttemptTrigger.INITIAL)));
        when(notifications.saveIfStatus(any(), any(), any(Integer.class), any(Integer.class)))
                .thenReturn(true);

        NotificationDeliveryDecision auto =
                service.prepare(event(1, AttemptTrigger.INITIAL), AT.plusSeconds(2));

        assertThat(auto.idempotencyKey()).isEqualTo("41:1");

        // **정적 팩토리를 직접 부르지 않는다.** send(notification, 2, 2, MANUAL) 로
        // 단언하면 baseAttemptSeq 를 시험이 손으로 넣는 셈이라, prepare 가 엉뚱한
        // 값을 넘겨도 통과한다 — 값 객체의 문자열 조립만 태우는 시험이 된다.
        // 수동 재처리가 실제로 서 있는 상태(SENDING · 2번째 시도 · 그 시도의
        // 아웃박스가 MANUAL)를 만들어 prepare 에 태운다.
        Notification reprocessing = new Notification(41L, 10L, 20L, 100L,
                Notification.DEFAULT_CHANNEL, NotificationStatus.SENDING, 2, 1, null,
                "member:20", "coupon-issued:100", AT, AT, null, AT);
        when(notifications.findById(41L)).thenReturn(Optional.of(reprocessing));
        when(outboxes.findTriggerByNotificationIdAndAttemptSeq(41L, 2))
                .thenReturn(Optional.of(AttemptTrigger.MANUAL));

        NotificationDeliveryDecision manual =
                service.prepare(event(2, AttemptTrigger.MANUAL), AT.plusSeconds(60));

        // 같은 알림이라도 **시도 번호**가 다르면 키가 다르다 — 그것이 "새 발송" 의 표시다.
        assertThat(manual.action()).isEqualTo(Action.SEND);
        assertThat(manual.idempotencyKey()).isEqualTo("41:2");
    }

    private static Notification pending() {
        return new Notification(41L, 10L, 20L, 100L, Notification.DEFAULT_CHANNEL,
                NotificationStatus.PENDING, 0, 0, null, "member:20", "coupon-issued:100",
                AT, AT, null, null);
    }

    private static NotificationAttempt failedAttempt(int sequence, AttemptTrigger trigger) {
        return new NotificationAttempt((long) sequence, 41L, sequence, trigger,
                AttemptResult.FAILED, NotifyFailureReason.SEND_TIMEOUT,
                AT, AT.plusSeconds(1), AT.plusSeconds(1));
    }

    private static NotificationRequestedEvent event(int sequence, AttemptTrigger trigger) {
        return new NotificationRequestedEvent(41L, 20L, 10L, sequence, trigger, AT);
    }
}

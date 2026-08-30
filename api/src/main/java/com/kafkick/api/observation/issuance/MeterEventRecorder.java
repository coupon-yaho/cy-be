package com.kafkick.api.observation.issuance;

import java.time.Instant;
import java.time.Duration;
import java.util.Objects;
import java.util.OptionalLong;

import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.IssuanceFlowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records the local, lossless coupon-round meters from OBS-24 flow events.
 *
 * <p>{@code ISSUE_ATTEMPT} is the one meter that counts replays: it is a stage, not a result, so every
 * retry increments it (see {@code EventType}). Every other meter here — the per-coupon-round success and
 * admitted counters and {@code app.issuance.outcome} — skips replays. {@code app.issuance.outcome} in
 * particular must stay one increment per logical result — a replay re-emits a stored result rather than computing a new one, so counting it
 * inflates the denominator of every issue-rate and failure-rate panel with no exception and no log.
 * That is why both result paths test {@code replayed()} before the {@code reasonCode} branch.
 */
public final class MeterEventRecorder implements EventRecorder {

    private static final Logger log = LoggerFactory.getLogger(MeterEventRecorder.class);
    private final CouponRoundMeterRegistry couponRoundMeters;
    private final FailureLogThrottle failureLog;

    public MeterEventRecorder(CouponRoundMeterRegistry couponRoundMeters, Duration failureLogInterval) {
        this.couponRoundMeters = Objects.requireNonNull(couponRoundMeters, "couponRoundMeters");
        this.failureLog = new FailureLogThrottle(failureLogInterval);
    }

    @Override
    public void record(IssuanceFlowEvent event) {
        try {
            recordSafely(Objects.requireNonNull(event, "event"));
        } catch (RuntimeException exception) {
            logFailureAtMostOncePerInterval(event, exception);
        }
    }

    private void recordSafely(IssuanceFlowEvent event) {
        switch (event.eventType()) {
            case ISSUE_ATTEMPT -> couponRoundMeters.couponRoundMeters(event.couponId())
                    .ifPresent(meters -> meters.attempt().increment());
            case QUEUE_ADMITTED -> {
                couponRoundMeters.couponRoundMeters(event.couponId()).ifPresent(meters -> {
                    meters.admitted().increment();
                    meters.lastAdmittedEpoch().accumulateAndGet(epochSeconds(event.occurredAt()), Math::max);
                });
            }
            case ISSUE_RESULT -> recordIssueResult(event);
            case ENTRY_RESULT -> recordEntryResult(event);
            default -> throw new IllegalArgumentException(
                    "Unsupported issuance observation event type: " + event.eventType());
        }
    }

    private void recordIssueResult(IssuanceFlowEvent event) {
        if (event.replayed()) {
            return;
        }
        if (event.reasonCode() != null) {
            couponRoundMeters.recordRejectedOutcome(event.reasonCode());
            return;
        }
        couponRoundMeters.couponRoundMeters(event.couponId()).ifPresent(meters -> {
            meters.success().increment();
            meters.lastSuccessEpoch().accumulateAndGet(epochSeconds(event.occurredAt()), Math::max);
        });
        couponRoundMeters.recordIssuedOutcome();
    }

    private void recordEntryResult(IssuanceFlowEvent event) {
        if (event.replayed()) {
            return;
        }
        if (event.reasonCode() != null) {
            couponRoundMeters.recordRejectedOutcome(event.reasonCode());
            return;
        }
        if (event.queueSequence() != null) {
            couponRoundMeters.recordQueuedOutcome();
        }
        // Immediate admission is followed by ISSUE_ATTEMPT; it has no distinct outcome label.
    }

    private static long epochSeconds(Instant occurredAt) {
        return occurredAt.getEpochSecond();
    }

    private void logFailureAtMostOncePerInterval(
            IssuanceFlowEvent event,
            RuntimeException exception
    ) {
        OptionalLong total = failureLog.recordFailure();
        if (total.isEmpty()) {
            return;
        }
        log.warn("쿠폰 회차 발급 미터 기록에 실패했습니다. 업무 흐름은 계속 진행합니다. "
                        + "누적 {}건, eventType={}, couponId={}, cause={}",
                total.getAsLong(),
                event == null ? null : event.eventType(),
                event == null ? null : event.couponId(),
                exception.getClass().getSimpleName());
    }

}

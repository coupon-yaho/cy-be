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
 * Records the local, lossless campaign meters from OBS-24 flow events.
 *
 * <p>Stage meters count replays; outcome meters do not. {@code ISSUE_ATTEMPT} is a stage and counts
 * every retry (see {@code EventType}), while {@code app.issuance.outcome} must stay one increment per
 * logical result — a replay re-emits a stored result rather than computing a new one, so counting it
 * inflates the denominator of every issue-rate and failure-rate panel with no exception and no log.
 * That is why both result paths test {@code replayed()} before the {@code reasonCode} branch.
 */
public final class MeterEventRecorder implements EventRecorder {

    private static final Logger log = LoggerFactory.getLogger(MeterEventRecorder.class);
    private final CampaignMeterRegistry campaignMeters;
    private final FailureLogThrottle failureLog;

    public MeterEventRecorder(CampaignMeterRegistry campaignMeters, Duration failureLogInterval) {
        this.campaignMeters = Objects.requireNonNull(campaignMeters, "campaignMeters");
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
            case ISSUE_ATTEMPT -> campaignMeters.campaignMeters(event.couponId())
                    .ifPresent(meters -> meters.attempt().increment());
            case QUEUE_ADMITTED -> {
                campaignMeters.campaignMeters(event.couponId()).ifPresent(meters -> {
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
            campaignMeters.recordRejectedOutcome(event.reasonCode());
            return;
        }
        campaignMeters.campaignMeters(event.couponId()).ifPresent(meters -> {
            meters.success().increment();
            meters.lastSuccessEpoch().accumulateAndGet(epochSeconds(event.occurredAt()), Math::max);
        });
        campaignMeters.recordIssuedOutcome();
    }

    private void recordEntryResult(IssuanceFlowEvent event) {
        if (event.replayed()) {
            return;
        }
        if (event.reasonCode() != null) {
            campaignMeters.recordRejectedOutcome(event.reasonCode());
            return;
        }
        if (event.queueSequence() != null) {
            campaignMeters.recordQueuedOutcome();
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
        log.warn("캠페인 발급 미터 기록에 실패했습니다. 업무 흐름은 계속 진행합니다. "
                        + "누적 {}건, eventType={}, couponId={}, cause={}",
                total.getAsLong(),
                event == null ? null : event.eventType(),
                event == null ? null : event.couponId(),
                exception.getClass().getSimpleName());
    }

}

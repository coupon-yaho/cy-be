package com.kafkick.api.observation.issuance;

import java.time.Instant;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicLong;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.ReasonCode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Records the local, lossless campaign meters from OBS-24 flow events. */
public final class MeterEventRecorder implements EventRecorder {

    private static final Logger log = LoggerFactory.getLogger(MeterEventRecorder.class);
    private static final String TAG_COUPON_ID = "coupon_id";
    private static final String TAG_STAGE = "stage";
    private static final String TAG_OUTCOME = "outcome";
    private static final long NO_EVENT_EPOCH = Long.MIN_VALUE;

    private final MeterRegistry meterRegistry;
    private final Map<Long, CampaignMeters> campaigns = new ConcurrentHashMap<>();
    private final Map<Long, ReentrantLock> campaignRegistrationLocks = new ConcurrentHashMap<>();
    private final Map<ReasonCode, Counter> rejectedOutcomes = new EnumMap<>(ReasonCode.class);
    private final Counter issuedOutcome;
    private final Counter queuedOutcome;
    private final Counter campaignLimitExceeded;
    private final long failureLogIntervalNanos;
    private final AtomicLong recordFailures = new AtomicLong();
    private final AtomicLong nextFailureLogAtNanos = new AtomicLong(System.nanoTime());

    public MeterEventRecorder(MeterRegistry meterRegistry) {
        this(meterRegistry, IssuanceObservationService.DEFAULT_LOG_INTERVAL);
    }

    public MeterEventRecorder(MeterRegistry meterRegistry, Duration failureLogInterval) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        this.failureLogIntervalNanos = requirePositive(failureLogInterval).toNanos();
        for (ReasonCode reasonCode : ReasonCode.values()) {
            rejectedOutcomes.put(reasonCode, outcomeCounter(reasonCode.name()));
        }
        issuedOutcome = outcomeCounter("ISSUED");
        queuedOutcome = outcomeCounter("QUEUED");
        campaignLimitExceeded = Counter.builder(MeterNames.CAMPAIGN_LIMIT_EXCEEDED)
                .description("Campaign meter registrations rejected by the OBS-26 cardinality limit")
                .register(meterRegistry);
    }

    @Override
    public void record(IssuanceFlowEvent event) {
        try {
            recordSafely(Objects.requireNonNull(event, "event"));
        } catch (RuntimeException exception) {
            logFailureAtMostOncePerInterval(event, exception);
        }
    }

    /** OBS-26 calls this when its single registration gate rejects a new campaign. */
    public void recordCampaignLimitExceeded() {
        try {
            campaignLimitExceeded.increment();
        } catch (RuntimeException exception) {
            logFailureAtMostOncePerInterval(null, exception);
        }
    }

    private void recordSafely(IssuanceFlowEvent event) {
        switch (event.eventType()) {
            case ISSUE_ATTEMPT -> campaignMeters(event.couponId()).attempt.increment();
            case QUEUE_ADMITTED -> {
                CampaignMeters meters = campaignMeters(event.couponId());
                meters.admitted.increment();
                meters.lastAdmittedEpoch.accumulateAndGet(epochSeconds(event.occurredAt()), Math::max);
            }
            case ISSUE_RESULT -> recordIssueResult(event);
            case ENTRY_RESULT -> recordEntryResult(event);
            default -> throw new IllegalArgumentException(
                    "Unsupported issuance observation event type: " + event.eventType());
        }
    }

    private void recordIssueResult(IssuanceFlowEvent event) {
        if (event.reasonCode() != null) {
            rejectedOutcomes.get(event.reasonCode()).increment();
            return;
        }
        if (event.replayed()) {
            return;
        }
        CampaignMeters meters = campaignMeters(event.couponId());
        meters.success.increment();
        meters.lastSuccessEpoch.accumulateAndGet(epochSeconds(event.occurredAt()), Math::max);
        issuedOutcome.increment();
    }

    private void recordEntryResult(IssuanceFlowEvent event) {
        if (event.reasonCode() != null) {
            rejectedOutcomes.get(event.reasonCode()).increment();
            return;
        }
        if (event.replayed()) {
            return;
        } else if (event.queueSequence() != null) {
            queuedOutcome.increment();
        }
        // Immediate admission is followed by ISSUE_ATTEMPT; it has no distinct outcome label.
    }

    /**
     * The only couponId-to-meter registration path.
     *
     * <p>OBS-26 owns the registration cap, retirement, and
     * {@link #recordCampaignLimitExceeded()} invocation. This ticket intentionally only exposes
     * the hook so that cap enforcement cannot later be added through a second registration path.
     */
    private CampaignMeters campaignMeters(long couponId) {
        CampaignMeters existing = campaigns.get(couponId);
        if (existing != null) {
            return existing;
        }

        ReentrantLock registrationLock = campaignRegistrationLocks.computeIfAbsent(
                couponId, ignored -> new ReentrantLock());
        registrationLock.lock();
        try {
            existing = campaigns.get(couponId);
            if (existing != null) {
                return existing;
            }
            CampaignMeters registered = registerCampaignMeters(couponId);
            campaigns.put(couponId, registered);
            return registered;
        } finally {
            registrationLock.unlock();
            campaignRegistrationLocks.remove(couponId, registrationLock);
        }
    }

    private CampaignMeters registerCampaignMeters(long couponId) {
        String couponIdTag = Long.toString(couponId);
        Counter attempt = Counter.builder(MeterNames.ISSUANCE_FLOW)
                .description("Policy-passed engine entries; not a success-rate denominator")
                .tag(TAG_COUPON_ID, couponIdTag).tag(TAG_STAGE, "attempt").register(meterRegistry);
        Counter success = Counter.builder(MeterNames.ISSUANCE_FLOW)
                .description("Successfully issued coupons")
                .tag(TAG_COUPON_ID, couponIdTag).tag(TAG_STAGE, "success").register(meterRegistry);
        Counter admitted = Counter.builder(MeterNames.QUEUE_ADMITTED)
                .description("Queue admissions confirmed for a campaign")
                .tag(TAG_COUPON_ID, couponIdTag).register(meterRegistry);
        AtomicLong lastSuccessEpoch = new AtomicLong(NO_EVENT_EPOCH);
        AtomicLong lastAdmittedEpoch = new AtomicLong(NO_EVENT_EPOCH);
        Gauge.builder(MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH, lastSuccessEpoch,
                        MeterEventRecorder::epochValue)
                .description("Epoch seconds of the latest successful issuance application event")
                .tag(TAG_COUPON_ID, couponIdTag).strongReference(true).register(meterRegistry);
        Gauge.builder(MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH, lastAdmittedEpoch,
                        MeterEventRecorder::epochValue)
                .description("Epoch seconds of the latest queue admission application event")
                .tag(TAG_COUPON_ID, couponIdTag).strongReference(true).register(meterRegistry);
        return new CampaignMeters(attempt, success, admitted, lastSuccessEpoch, lastAdmittedEpoch);
    }

    private Counter outcomeCounter(String outcome) {
        return Counter.builder(MeterNames.ISSUANCE_OUTCOME)
                .description("Flow outcomes across all campaigns; QUEUED and ISSUED are not mutually exclusive")
                .tag(TAG_OUTCOME, outcome).register(meterRegistry);
    }

    private static long epochSeconds(Instant occurredAt) {
        return occurredAt.getEpochSecond();
    }

    private void logFailureAtMostOncePerInterval(
            IssuanceFlowEvent event,
            RuntimeException exception
    ) {
        long total = recordFailures.incrementAndGet();
        long now = System.nanoTime();
        long due = nextFailureLogAtNanos.get();
        if (now - due < 0 || !nextFailureLogAtNanos.compareAndSet(
                due, now + failureLogIntervalNanos)) {
            return;
        }
        log.warn("캠페인 발급 미터 기록에 실패했습니다. 업무 흐름은 계속 진행합니다. "
                        + "누적 {}건, eventType={}, couponId={}, cause={}",
                total,
                event == null ? null : event.eventType(),
                event == null ? null : event.couponId(),
                exception.getClass().getSimpleName());
    }

    private static double epochValue(AtomicLong epoch) {
        long value = epoch.get();
        return value == NO_EVENT_EPOCH ? Double.NaN : value;
    }

    private static Duration requirePositive(Duration interval) {
        Objects.requireNonNull(interval, "failureLogInterval");
        if (interval.isZero() || interval.isNegative()) {
            throw new IllegalArgumentException("failureLogInterval must be positive");
        }
        return interval;
    }

    private record CampaignMeters(
            Counter attempt,
            Counter success,
            Counter admitted,
            AtomicLong lastSuccessEpoch,
            AtomicLong lastAdmittedEpoch
    ) {
    }
}

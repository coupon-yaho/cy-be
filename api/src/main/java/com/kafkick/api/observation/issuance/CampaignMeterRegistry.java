package com.kafkick.api.observation.issuance;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.core.observation.ReasonCode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The sole gate for campaign-meter registration, retirement, and delayed-event suppression. */
public final class CampaignMeterRegistry implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CampaignMeterRegistry.class);
    private static final String TAG_COUPON_ID = "coupon_id";
    private static final String TAG_STAGE = "stage";
    private static final long NO_EVENT_EPOCH = Long.MIN_VALUE;

    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final ScheduledExecutorService retirementExecutor;
    private final int maxActiveCampaigns;
    private final Duration retireGracePeriod;
    private final Duration tombstoneRetention;
    private final int tombstoneMaxEntries;
    private final FailureLogThrottle failureLog;
    private final Counter campaignLimitExceeded;
    private final EnumMap<ReasonCode, Counter> rejectedOutcomes = new EnumMap<>(ReasonCode.class);
    private final Counter issuedOutcome;
    private final Counter queuedOutcome;
    private final Map<Long, CampaignMeters> campaigns = new ConcurrentHashMap<>();
    // Registration, cap accounting, retirement, and tombstone insertion must be one atomic state transition.
    private final ReentrantLock registrationGate = new ReentrantLock();
    private final Map<Long, Boolean> retirementScheduled = new ConcurrentHashMap<>();
    private final LinkedHashMap<Long, Instant> tombstones = new LinkedHashMap<>();

    public CampaignMeterRegistry(
            MeterRegistry meterRegistry,
            CampaignMeterProperties properties,
            Duration failureLogInterval
    ) {
        this(meterRegistry, properties, failureLogInterval, Clock.systemUTC(),
                Executors.newSingleThreadScheduledExecutor(runnable -> {
                    Thread thread = new Thread(runnable, "campaign-meter-retirement");
                    thread.setDaemon(true);
                    return thread;
                }));
    }

    CampaignMeterRegistry(
            MeterRegistry meterRegistry,
            CampaignMeterProperties properties,
            Duration failureLogInterval,
            Clock clock,
            ScheduledExecutorService retirementExecutor
    ) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
        Objects.requireNonNull(properties, "properties");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.retirementExecutor = Objects.requireNonNull(retirementExecutor, "retirementExecutor");
        maxActiveCampaigns = properties.resolvedMaxActiveCampaigns();
        retireGracePeriod = properties.resolvedRetireGracePeriod();
        tombstoneRetention = properties.resolvedTombstoneRetention();
        tombstoneMaxEntries = properties.resolvedTombstoneMaxEntries();
        failureLog = new FailureLogThrottle(failureLogInterval);
        campaignLimitExceeded = Counter.builder(MeterNames.CAMPAIGN_LIMIT_EXCEEDED)
                .description("Campaign meter registrations rejected by the cardinality limit")
                .register(meterRegistry);
        for (ReasonCode reasonCode : ReasonCode.values()) {
            rejectedOutcomes.put(reasonCode, outcomeCounter(reasonCode.name()));
        }
        issuedOutcome = outcomeCounter("ISSUED");
        queuedOutcome = outcomeCounter("QUEUED");
    }

    public Optional<CampaignMeters> campaignMeters(long couponId) {
        CampaignMeters existing = campaigns.get(couponId);
        if (existing != null) {
            return Optional.of(existing);
        }
        registrationGate.lock();
        try {
            existing = campaigns.get(couponId);
            if (existing != null) {
                return Optional.of(existing);
            }
            if (isTombstoned(couponId)) {
                return Optional.empty();
            }
            if (campaigns.size() >= maxActiveCampaigns) {
                campaignLimitExceeded.increment();
                logAtMostOnce("캠페인 미터 등록 상한으로 거부했습니다. couponId={}", couponId, null);
                return Optional.empty();
            }
            CampaignMeters registered = registerCampaignMeters(couponId);
            campaigns.put(couponId, registered);
            return Optional.of(registered);
        } finally {
            registrationGate.unlock();
        }
    }

    public void retireCampaign(long couponId, Instant closedAt) {
        try {
            if (couponId <= 0 || closedAt == null) {
                logAtMostOnce("캠페인 수명 통지의 값 계약을 위반했습니다. campaignCouponId={}, closedAt={}",
                        couponId, closedAt, null);
                return;
            }
            if (isTombstoned(couponId)) {
                return;
            }
            if (retirementScheduled.putIfAbsent(couponId, Boolean.TRUE) != null) {
                return;
            }
            Instant retireAt = closedAt.plus(retireGracePeriod);
            long delayMillis = Math.max(0, Duration.between(clock.instant(), retireAt).toMillis());
            retirementExecutor.schedule(() -> retireNow(couponId), delayMillis, TimeUnit.MILLISECONDS);
        } catch (RuntimeException exception) {
            retirementScheduled.remove(couponId);
            logAtMostOnce("캠페인 미터 retire 처리에 실패했습니다. couponId={}", couponId, exception);
        }
    }

    public void recordRejectedOutcome(ReasonCode reasonCode) {
        rejectedOutcomes.get(reasonCode).increment();
    }

    public void recordIssuedOutcome() {
        issuedOutcome.increment();
    }

    public void recordQueuedOutcome() {
        queuedOutcome.increment();
    }

    private void retireNow(long couponId) {
        registrationGate.lock();
        try {
            // Tombstone first: an instrumentation failure must not make a closed campaign registrable again.
            addTombstone(couponId);
            CampaignMeters removed = campaigns.remove(couponId);
            if (removed != null) {
                removeCampaignMeters(couponId, removed);
            }
        } catch (RuntimeException exception) {
            logAtMostOnce("캠페인 미터 retire 처리에 실패했습니다. couponId={}", couponId, exception);
        } finally {
            retirementScheduled.remove(couponId);
            registrationGate.unlock();
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
        Gauge successGauge = Gauge.builder(MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH, lastSuccessEpoch,
                        CampaignMeterRegistry::epochValue)
                .description("Epoch seconds of the latest successful issuance application event")
                .tag(TAG_COUPON_ID, couponIdTag).strongReference(true).register(meterRegistry);
        Gauge admittedGauge = Gauge.builder(MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH, lastAdmittedEpoch,
                        CampaignMeterRegistry::epochValue)
                .description("Epoch seconds of the latest queue admission application event")
                .tag(TAG_COUPON_ID, couponIdTag).strongReference(true).register(meterRegistry);
        return new CampaignMeters(attempt, success, admitted, lastSuccessEpoch, lastAdmittedEpoch,
                List.of(attempt, success, admitted, successGauge, admittedGauge));
    }

    private Counter outcomeCounter(String outcome) {
        return Counter.builder(MeterNames.ISSUANCE_OUTCOME)
                .description("Flow outcomes across all campaigns; QUEUED and ISSUED are not mutually exclusive")
                .tag("outcome", outcome).register(meterRegistry);
    }

    private boolean isTombstoned(long couponId) {
        synchronized (tombstones) {
            Instant expiry = tombstones.get(couponId);
            if (expiry == null) {
                return false;
            }
            if (expiry.isAfter(clock.instant())) {
                return true;
            }
            tombstones.remove(couponId);
            return false;
        }
    }

    private void removeCampaignMeters(long couponId, CampaignMeters removed) {
        for (Meter meter : removed.meters()) {
            try {
                meterRegistry.remove(meter);
            } catch (RuntimeException exception) {
                // Keep attempting the remaining meters so one registry failure cannot retain the full label set.
                logAtMostOnce("캠페인 미터 retire 제거에 실패했습니다. couponId={}", couponId, exception);
            }
        }
    }

    private void addTombstone(long couponId) {
        synchronized (tombstones) {
            Instant now = clock.instant();
            pruneTombstones(now);
            tombstones.put(couponId, now.plus(tombstoneRetention));
            while (tombstones.size() > tombstoneMaxEntries) {
                tombstones.remove(tombstones.keySet().iterator().next());
            }
        }
    }

    private void pruneTombstones(Instant now) {
        tombstones.entrySet().removeIf(entry -> !entry.getValue().isAfter(now));
    }

    private void logAtMostOnce(String message, long couponId, Instant closedAt, RuntimeException exception) {
        OptionalLong total = failureLog.recordFailure();
        if (total.isPresent()) {
            log.warn(message + ", 누적 {}건, cause={}", couponId, closedAt, total.getAsLong(),
                    exception == null ? null : exception.getClass().getSimpleName());
        }
    }

    private void logAtMostOnce(String message, long couponId, RuntimeException exception) {
        OptionalLong total = failureLog.recordFailure();
        if (total.isPresent()) {
            log.warn(message + ", 누적 {}건, cause={}", couponId, total.getAsLong(),
                    exception == null ? null : exception.getClass().getSimpleName());
        }
    }

    static double epochValue(AtomicLong epoch) {
        long value = epoch.get();
        return value == NO_EVENT_EPOCH ? Double.NaN : value;
    }

    @Override
    public void close() {
        retirementExecutor.shutdownNow();
    }

    public record CampaignMeters(
            Counter attempt,
            Counter success,
            Counter admitted,
            AtomicLong lastSuccessEpoch,
            AtomicLong lastAdmittedEpoch,
            List<Meter> meters
    ) {
    }
}

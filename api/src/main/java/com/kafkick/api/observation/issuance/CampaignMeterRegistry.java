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
    private final Duration retirementRetryDelay;
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
    // Guarded by registrationGate: an unsuccessful remove must retain its meter references for retry.
    private final Map<Long, PendingRetirement> pendingRetirements = new java.util.HashMap<>();
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
        retirementRetryDelay = Objects.requireNonNull(failureLogInterval, "failureLogInterval");
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
            if (pendingRetirements.containsKey(couponId) || isTombstoned(couponId)) {
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
            registrationGate.lock();
            try {
                if (isTombstoned(couponId)) {
                    if (pendingRetirements.containsKey(couponId)) {
                        scheduleRetirement(couponId, retryDelayMillis());
                    }
                    return;
                }
                Instant retireAt = closedAt.plus(retireGracePeriod);
                long delayMillis = Math.max(0, Duration.between(clock.instant(), retireAt).toMillis());
                scheduleRetirement(couponId, delayMillis);
            } finally {
                registrationGate.unlock();
            }
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

    /**
     * 미터를 실제로 걷어낸다. 실패하면 <b>남은 것을 들고 다시 예약한다.</b>
     *
     * <p>{@code removeCampaignMeters} 는 미터 하나의 제거 실패만 삼킨다(CY-435). 그 바깥에서
     * 나는 예외 — tombstone 삽입, 맵 조작, 예약 거부 — 는 이 catch 로 온다. 여기서 로그만 남기면
     * 남은 미터 회수가 그 캠페인에 대해 영원히 멈추고, 죽은 캠페인의 시계열이 계속 scrape 된다.
     *
     * <p><b>참조를 먼저 맡기는 것은 불변식이지 현재 도달 가능한 방어가 아니다.</b>
     * {@code campaigns.remove()} 로 꺼낸 목록을 {@code pendingRetirements} 에 넣기 전에 던지면 그
     * 캠페인은 두 맵 어디에도 없는 상태가 되고, 재시도가 와도 {@code pending} 은 null ·
     * {@code campaigns.remove()} 도 null 이라 그 자리에서 돌아간다 — 미터는 레지스트리에 남은 채
     * 참조를 잃고 되찾을 방법이 없다. <b>다만 지금 그 구간에서 던지는 경로는 없다</b> —
     * {@code removeCampaignMeters} 가 미터별 {@code RuntimeException} 을 전부 삼키기 때문이다.
     * 그래서 이 순서를 깨뜨려 실패하는 테스트를 쓸 수 없다. 순서를 이렇게 둔 것은 그 구간에
     * 나중에 던지는 코드가 들어와도 불변식이 유지되게 하기 위한 것이고, 지금 무언가를 막고
     * 있다고 읽으면 안 된다.
     *
     * <p>이 방어가 만드는 반대 방향 실패 — 계속 실패하는 원인(예: 레지스트리가 영구 거부)이면
     * {@code retirementRetryDelay} 간격으로 무한히 재예약한다. 멈추지 않는 쪽을 고른 이유는
     * 실패의 대부분이 일시적이고, 끊으면 그 캠페인이 재기동 전까지 회수되지 않기 때문이다.
     * 반복은 {@link FailureLogThrottle} 이 로그를 간격당 한 줄로 눌러 조용히 돈다.
     */
    private void retireNow(long couponId) {
        registrationGate.lock();
        try {
            retirementScheduled.remove(couponId);
            PendingRetirement pending = pendingRetirements.get(couponId);
            if (pending == null) {
                // Tombstone first: an instrumentation failure must not make a closed campaign registrable again.
                addTombstone(couponId);
                CampaignMeters removed = campaigns.remove(couponId);
                if (removed == null) {
                    return;
                }
                pending = new PendingRetirement(removed, removed.meters());
                // 제거를 시도하기 전에 맡긴다 — 아래에서 터져도 참조가 살아 있어야 재시도가 뭔가를 붙잡는다.
                pendingRetirements.put(couponId, pending);
            }
            List<Meter> remaining = removeCampaignMeters(couponId, pending.remainingMeters());
            if (remaining.isEmpty()) {
                pendingRetirements.remove(couponId);
            } else {
                pendingRetirements.put(couponId, new PendingRetirement(pending.campaignMeters(), remaining));
                scheduleRetirement(couponId, retryDelayMillis());
            }
        } catch (RuntimeException exception) {
            logAtMostOnce("캠페인 미터 retire 처리에 실패했습니다. couponId={}", couponId, exception);
            rescheduleAfterFailure(couponId);
        } finally {
            registrationGate.unlock();
        }
    }

    /**
     * 실패한 회수를 다시 예약한다. <b>여기서 나가는 예외는 없다.</b>
     *
     * <p>바깥 예외의 원인이 예약 거부 그 자체일 수 있다 — 실행기가 종료 중이면
     * {@code scheduleRetirement} 가 {@code RejectedExecutionException} 을 던지고, 그것을 그대로
     * 흘리면 catch 블록이 던지는 꼴이 되어 원래 원인이 이 예외에 덮인다.
     *
     * <p>회수할 것이 남았을 때만 예약한다. 두 맵 어디에도 없으면 할 일이 없다 — 조건 없이
     * 예약하면 이미 끝난 캠페인이 지연 간격마다 깨어나 아무것도 안 하고 다시 잠든다.
     */
    private void rescheduleAfterFailure(long couponId) {
        if (!pendingRetirements.containsKey(couponId) && !campaigns.containsKey(couponId)) {
            return;
        }
        try {
            scheduleRetirement(couponId, retryDelayMillis());
        } catch (RuntimeException rejected) {
            logAtMostOnce("캠페인 미터 retire 재예약에 실패했습니다. couponId={}", couponId, rejected);
        }
    }

    private void scheduleRetirement(long couponId, long delayMillis) {
        if (retirementScheduled.putIfAbsent(couponId, Boolean.TRUE) != null) {
            return;
        }
        try {
            retirementExecutor.schedule(() -> retireNow(couponId), delayMillis, TimeUnit.MILLISECONDS);
        } catch (RuntimeException exception) {
            retirementScheduled.remove(couponId);
            throw exception;
        }
    }

    private long retryDelayMillis() {
        return Math.max(1, retirementRetryDelay.toMillis());
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

    private List<Meter> removeCampaignMeters(long couponId, List<Meter> meters) {
        List<Meter> remaining = new java.util.ArrayList<>();
        for (Meter meter : meters) {
            try {
                meterRegistry.remove(meter);
            } catch (RuntimeException exception) {
                remaining.add(meter);
                logAtMostOnce("캠페인 미터 retire 제거에 실패했습니다. couponId={}", couponId, exception);
            }
        }
        return remaining;
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

    private record PendingRetirement(CampaignMeters campaignMeters, List<Meter> remainingMeters) {
    }
}

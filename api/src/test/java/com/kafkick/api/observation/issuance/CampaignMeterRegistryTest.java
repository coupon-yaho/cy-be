package com.kafkick.api.observation.issuance;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReleaseStage;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import org.junit.jupiter.api.Test;

class CampaignMeterRegistryTest {

    @Test
    void rejectsNewCampaignsAtTheCapWithoutCreatingTheirMeters() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters, 1);
        MeterEventRecorder recorder = recorder(campaigns, meters);
        try {
            IssuanceFlowEventFactory factory = factory();
            recorder.record(factory.issueAttempt(context(201L)));
            recorder.record(factory.issueAttempt(context(202L)));
            recorder.record(factory.issued(context(202L), 1L, "coupon-code-0001"));
            recorder.record(factory.issueAttempt(context(201L)));

            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters())
                    .hasSize(2);
            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201")
                    .tag("stage", "attempt").counter().count()).isEqualTo(2.0);
            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "202").counters()).isEmpty();
            assertThat(meters.find(MeterNames.ISSUANCE_OUTCOME).tag("outcome", "ISSUED")
                    .counter().count()).isEqualTo(1.0);
            assertThat(meters.find(MeterNames.CAMPAIGN_LIMIT_EXCEEDED).counter().count()).isEqualTo(2.0);
        } finally {
            campaigns.close();
        }
    }

    @Test
    void retiresAllCampaignScopedMetersAndTombstonesDelayedEvents() throws Exception {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters, 1);
        MeterEventRecorder recorder = recorder(campaigns, meters);
        IssuanceFlowEventFactory factory = factory();

        try {
            recorder.record(factory.issueAttempt(context(201L)));
            recorder.record(factory.admitted(context(201L), 1L));
            recorder.record(factory.issued(context(201L), 1L, "code"));
            campaigns.retireCampaign(201L, Instant.now().minusSeconds(1));

            awaitNoCampaignMeters(meters, "201");
            recorder.record(factory.issueAttempt(context(201L)));
            assertNoCampaignMeters(meters, "201");

            recorder.record(factory.issueAttempt(context(202L)));
            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "202").counters()).hasSize(2);
        } finally {
            campaigns.close();
        }
    }

    @Test
    void schedulesOnlyOneRetirementForRepeatedNotifications() {
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        CampaignMeterRegistry campaigns = new CampaignMeterRegistry(meters,
                new CampaignMeterProperties(1, Duration.ofHours(1), Duration.ofHours(1), 10),
                Duration.ofSeconds(10), Clock.systemUTC(), executor);
        try {
            Instant closedAt = Instant.now();
            campaigns.retireCampaign(201L, closedAt);
            campaigns.retireCampaign(201L, closedAt);

            assertThat(executor.getQueue()).hasSize(1);
        } finally {
            campaigns.close();
        }
    }

    @Test
    void registersOneCampaignMeterSetUnderConcurrentFirstEvents() throws Exception {
        CountingSimpleMeterRegistry meters = new CountingSimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters, 10);
        int countersBefore = meters.counterCreations.get();
        int gaugesBefore = meters.gaugeCreations.get();
        int concurrency = 32;
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch fire = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(concurrency);
        try {
            var futures = java.util.stream.IntStream.range(0, concurrency)
                    .mapToObj(ignored -> executor.submit(() -> {
                        ready.countDown();
                        try {
                            if (!fire.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("발사 신호가 오지 않았다");
                            }
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(exception);
                        }
                        campaigns.campaignMeters(201L).orElseThrow().attempt().increment();
                    })).toList();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            fire.countDown();
            for (var future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }

            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters())
                    .hasSize(2);
            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201")
                    .tag("stage", "attempt").counter().count()).isEqualTo(32.0);
            assertThat(meters.counterCreations).hasValue(countersBefore + 3);
            assertThat(meters.gaugeCreations).hasValue(gaugesBefore + 2);
        } finally {
            executor.shutdownNow();
            campaigns.close();
        }
    }

    @Test
    void evictsTheOldestTombstoneWhenItsBoundIsReached() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-24T00:00:00Z"));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters,
                new CampaignMeterProperties(2, Duration.ofMinutes(1), Duration.ofHours(1), 1), clock);
        MeterEventRecorder recorder = recorder(campaigns, meters);
        try {
            recorder.record(factory().issueAttempt(context(201L)));
            recorder.record(factory().issueAttempt(context(202L)));
            campaigns.retireCampaign(201L, clock.instant().minus(Duration.ofMinutes(1)));
            campaigns.retireCampaign(202L, clock.instant().minus(Duration.ofMinutes(1)));

            recorder.record(factory().issueAttempt(context(201L)));
            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters()).hasSize(2);
            recorder.record(factory().issueAttempt(context(202L)));
            assertNoCampaignMeters(meters, "202");
        } finally {
            campaigns.close();
        }
    }

    @Test
    void allowsRegistrationAfterTombstoneRetentionExpires() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-24T00:00:00Z"));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters,
                new CampaignMeterProperties(1, Duration.ofMinutes(1), Duration.ofMinutes(10), 10), clock);
        MeterEventRecorder recorder = recorder(campaigns, meters);
        try {
            recorder.record(factory().issueAttempt(context(201L)));
            campaigns.retireCampaign(201L, clock.instant().minus(Duration.ofMinutes(1)));
            recorder.record(factory().issueAttempt(context(201L)));
            assertNoCampaignMeters(meters, "201");

            clock.advance(Duration.ofMinutes(10).plusNanos(1));
            recorder.record(factory().issueAttempt(context(201L)));
            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters()).hasSize(2);
        } finally {
            campaigns.close();
        }
    }

    @Test
    void continuesRemovingOtherCampaignMetersWhenOneRemovalFails() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-24T00:00:00Z"));
        ThrowingRemoveMeterRegistry meters = new ThrowingRemoveMeterRegistry(MeterNames.ISSUANCE_FLOW);
        CampaignMeterRegistry campaigns = registry(meters,
                new CampaignMeterProperties(1, Duration.ofMinutes(1), Duration.ofHours(1), 10), clock);
        try {
            MeterEventRecorder recorder = recorder(campaigns, meters);
            recorder.record(factory().issueAttempt(context(201L)));
            campaigns.retireCampaign(201L, clock.instant().minus(Duration.ofMinutes(1)));

            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters()).hasSize(1);
            assertThat(meters.find(MeterNames.QUEUE_ADMITTED).tag("coupon_id", "201").counters()).isEmpty();
            assertThat(meters.find(MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH).tag("coupon_id", "201")
                    .gauges()).isEmpty();
            assertThat(meters.find(MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH).tag("coupon_id", "201")
                    .gauges()).isEmpty();
        } finally {
            campaigns.close();
        }
    }

    private static CampaignMeterRegistry registry(SimpleMeterRegistry meters, int cap) {
        return new CampaignMeterRegistry(meters,
                new CampaignMeterProperties(cap, Duration.ofMillis(1), Duration.ofHours(1), 10),
                Duration.ofSeconds(10));
    }

    private static CampaignMeterRegistry registry(
            SimpleMeterRegistry meters,
            CampaignMeterProperties properties,
            Clock clock
    ) {
        return new CampaignMeterRegistry(meters, properties, Duration.ofSeconds(10), clock,
                new ImmediateScheduledExecutor());
    }

    private static MeterEventRecorder recorder(CampaignMeterRegistry campaigns, SimpleMeterRegistry meters) {
        return new MeterEventRecorder(campaigns, Duration.ofSeconds(10));
    }

    private static IssuanceFlowEventFactory factory() {
        return new IssuanceFlowEventFactory(java.util.UUID::randomUUID);
    }

    private static IssuanceFlowEvent.Ctx context(long couponId) {
        return new IssuanceFlowEvent.Ctx("request", 101L, couponId, Grade.GOLD, false,
                Instant.parse("2026-08-23T00:00:00Z"), EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, 901L, "api-1");
    }

    private static void awaitNoCampaignMeters(SimpleMeterRegistry meters, String couponId)
            throws InterruptedException {
        for (int attempt = 0; attempt < 100; attempt++) {
            boolean gone = meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", couponId).counters().isEmpty()
                    && meters.find(MeterNames.QUEUE_ADMITTED).tag("coupon_id", couponId).counters().isEmpty()
                    && meters.find(MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH).tag("coupon_id", couponId)
                    .gauges().isEmpty()
                    && meters.find(MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH).tag("coupon_id", couponId)
                    .gauges().isEmpty();
            if (gone) {
                return;
            }
            Thread.sleep(10);
        }
        assertNoCampaignMeters(meters, couponId);
    }

    private static void assertNoCampaignMeters(SimpleMeterRegistry meters, String couponId) {
        assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", couponId).counters()).isEmpty();
        assertThat(meters.find(MeterNames.QUEUE_ADMITTED).tag("coupon_id", couponId).counters()).isEmpty();
        assertThat(meters.find(MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH).tag("coupon_id", couponId)
                .gauges()).isEmpty();
        assertThat(meters.find(MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH).tag("coupon_id", couponId)
                .gauges()).isEmpty();
    }

    private static final class CountingSimpleMeterRegistry extends SimpleMeterRegistry {

        private final AtomicInteger counterCreations = new AtomicInteger();
        private final AtomicInteger gaugeCreations = new AtomicInteger();

        @Override
        protected Counter newCounter(Meter.Id id) {
            counterCreations.incrementAndGet();
            return super.newCounter(id);
        }

        @Override
        protected <T> Gauge newGauge(Meter.Id id, T source, ToDoubleFunction<T> valueFunction) {
            gaugeCreations.incrementAndGet();
            return super.newGauge(id, source, valueFunction);
        }
    }

    private static final class ThrowingRemoveMeterRegistry extends SimpleMeterRegistry {

        private final String failingMeterName;
        private final AtomicBoolean failOnce = new AtomicBoolean(true);

        private ThrowingRemoveMeterRegistry(String failingMeterName) {
            this.failingMeterName = failingMeterName;
        }

        @Override
        public Meter remove(Meter meter) {
            if (failingMeterName.equals(meter.getId().getName()) && failOnce.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated removal failure");
            }
            return super.remove(meter);
        }
    }

    private static final class MutableClock extends Clock {

        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            if (!ZoneOffset.UTC.equals(zone)) {
                throw new IllegalArgumentException("UTC only");
            }
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }

        private void advance(Duration duration) {
            instant = instant.plus(duration);
        }
    }

    private static final class ImmediateScheduledExecutor extends AbstractExecutorService
            implements ScheduledExecutorService {

        private boolean shutdown;

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            command.run();
            return new CompletedScheduledFuture<>(null);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            try {
                return new CompletedScheduledFuture<>(callable.call());
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            shutdown = true;
            return java.util.List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    private record CompletedScheduledFuture<V>(V value) implements ScheduledFuture<V> {

        @Override public long getDelay(TimeUnit unit) { return 0; }
        @Override public int compareTo(Delayed other) { return 0; }
        @Override public boolean cancel(boolean mayInterruptIfRunning) { return false; }
        @Override public boolean isCancelled() { return false; }
        @Override public boolean isDone() { return true; }
        @Override public V get() { return value; }
        @Override public V get(long timeout, TimeUnit unit) { return value; }
    }
}

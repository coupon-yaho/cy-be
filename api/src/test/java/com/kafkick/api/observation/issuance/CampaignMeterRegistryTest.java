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
    void retiresAllCampaignScopedMetersAndTombstonesDelayedEvents() {
        MutableClock clock = new MutableClock(Instant.parse("2026-08-24T00:00:00Z"));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters,
                new CampaignMeterProperties(1, Duration.ofMinutes(1), Duration.ofHours(1), 10), clock);
        MeterEventRecorder recorder = recorder(campaigns, meters);
        IssuanceFlowEventFactory factory = factory();

        try {
            recorder.record(factory.issueAttempt(context(201L)));
            recorder.record(factory.admitted(context(201L), 1L));
            recorder.record(factory.issued(context(201L), 1L, "code"));
            campaigns.retireCampaign(201L, clock.instant().minus(Duration.ofMinutes(1)));

            assertNoCampaignMeters(meters, "201");
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

            assertNoCampaignMeters(meters, "201");
            assertThat(meters.find(MeterNames.QUEUE_ADMITTED).tag("coupon_id", "201").counters()).isEmpty();
            assertThat(meters.find(MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH).tag("coupon_id", "201")
                    .gauges()).isEmpty();
            assertThat(meters.find(MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH).tag("coupon_id", "201")
                    .gauges()).isEmpty();
        } finally {
            campaigns.close();
        }
    }

    /**
     * {@code removeCampaignMeters} <b>바깥</b>에서 난 예외도 회수를 멈추지 않는다.
     *
     * <p>CY-435 가 넣은 재시도는 미터 하나의 제거 실패만 덮는다 — 그건 그 메서드 안의 catch 다.
     * tombstone 삽입 · 맵 조작처럼 그 바깥에서 던지면 예전에는 로그 한 줄만 남고 끝나서, 그
     * 캠페인의 미터는 재기동 전까지 레지스트리에 남아 계속 scrape 됐다.
     *
     * <p>시계로 터뜨린다. {@code addTombstone} 이 {@code clock.instant()} 를 부르므로 그 호출을
     * 한 번 실패시키면 {@code campaigns.remove()} 에 닿기도 전에 바깥 catch 로 간다.
     */
    @Test
    void reschedulesRetirementWhenTheFailureIsOutsideMeterRemoval() {
        // 호출 순서: ① 아래 인자 평가 ② retireCampaign 의 지연 계산 ③ retireNow 의 addTombstone.
        //           ③ 을 터뜨려야 retireNow 의 바깥 catch 로 간다 — ② 는 retireCampaign 이 자기 catch 로 삼킨다.
        //           한 번만 실패시켜서, 재예약된 재시도는 성공하고 미터가 실제로 걷혔음을 볼 수 있게 한다.
        FailingClock clock = new FailingClock(Instant.parse("2026-08-24T00:00:00Z"), 3, 1);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters,
                new CampaignMeterProperties(1, Duration.ofMinutes(1), Duration.ofHours(1), 10), clock);
        try {
            MeterEventRecorder recorder = recorder(campaigns, meters);
            recorder.record(factory().issueAttempt(context(201L)));
            assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters()).isNotEmpty();

            campaigns.retireCampaign(201L, clock.instant().minus(Duration.ofMinutes(1)));

            assertThat(clock.failed()).as("바깥 예외가 실제로 났어야 한다").isTrue();
            assertNoCampaignMeters(meters, "201");
        } finally {
            campaigns.close();
        }
    }

    /**
     * 회수할 것이 없으면 재예약하지 않는다.
     *
     * <p>조건 없이 재예약하면 이미 끝난 캠페인이 지연 간격마다 깨어나 아무것도 안 하고 다시
     * 잠든다. {@code ImmediateScheduledExecutor} 는 예약을 인라인 실행하므로 그 상태는 테스트에서
     * 무한 재귀({@code StackOverflowError})로 드러난다.
     */
    @Test
    void doesNotRescheduleWhenNothingIsLeftToRetire() {
        // ③ 부터 계속 실패한다. 재시도도 같은 자리에서 터지므로, 재예약 조건이 없으면 멈추지 않는다.
        FailingClock clock = new FailingClock(Instant.parse("2026-08-24T00:00:00Z"), 3, Integer.MAX_VALUE);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = registry(meters,
                new CampaignMeterProperties(1, Duration.ofMinutes(1), Duration.ofHours(1), 10), clock);
        try {
            // 미터를 한 번도 등록하지 않았다. campaigns · pendingRetirements 둘 다 비어 있다.
            campaigns.retireCampaign(201L, clock.instant().minus(Duration.ofMinutes(1)));

            assertThat(clock.failed()).isTrue();
            assertNoCampaignMeters(meters, "201");
        } finally {
            campaigns.close();
        }
    }

    /**
     * 실패가 계속돼도 재시도가 <b>한 번에 하나씩</b>만 쌓인다.
     *
     * <p>{@code ImmediateScheduledExecutor} 는 예약을 인라인 실행한다. 그래서 기존 재예약
     * 테스트들은 "단일 스레드 재귀가 동작한다" 만 보여 줄 뿐, 실제
     * {@code ScheduledExecutorService} 에서 재시도가 <b>다른 스레드에서 지연 뒤에</b> 도는
     * 성질에 대해서는 아무 말도 하지 않는다. 그 차이가 문제가 되는 지점이 둘이다.
     *
     * <ul>
     *   <li>등록된 캠페인 + 계속 실패하는 원인이면, 인라인 실행기에서는 무한 재귀
     *       ({@code StackOverflowError})가 되지만 실운영에서는 지연 간격마다 한 번 도는
     *       느린 루프다. 앞의 모양만 보고 "재귀가 터진다" 고 읽으면 안 된다.</li>
     *   <li>재시도가 큐에 앉아 있는 동안 같은 회차로 이벤트가 하나 더 들어오면
     *       {@code campaignMeters()} 가 미터를 <b>다시 등록</b>할 수 있다. 그 뒤 재시도가 돌아
     *       {@code campaigns.remove} 로 새로 만든 미터를 걷어 가면 살아 있는 회차의 시계열이
     *       조용히 끊긴다.</li>
     * </ul>
     *
     * <p>그래서 예약을 큐에 담고 테스트가 직접 드레인한다.
     */
    @Test
    void queuesAtMostOneRetryAtATimeWhileTheFailurePersists() {
        // 호출 순서: ① retireCampaign 의 지연 계산 ② retireNow 의 addTombstone.
        //           테스트는 시계를 직접 부르지 않는다(닫는 시각이 리터럴이다).
        FailingClock clock = new FailingClock(Instant.parse("2026-08-24T00:00:00Z"), 2, Integer.MAX_VALUE);
        QueueingScheduledExecutor executor = new QueueingScheduledExecutor();
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = new CampaignMeterRegistry(meters,
                new CampaignMeterProperties(1, Duration.ofMinutes(1), Duration.ofHours(1), 10),
                Duration.ofSeconds(10), clock, executor);
        try {
            MeterEventRecorder recorder = recorder(campaigns, meters);
            recorder.record(factory().issueAttempt(context(201L)));

            campaigns.retireCampaign(201L, Instant.parse("2026-08-23T23:59:00Z"));

            // 첫 예약 하나. 드레인할 때마다 실패하고 정확히 하나를 다시 예약한다.
            for (int round = 0; round < 5; round++) {
                assertThat(executor.pending())
                        .as("라운드 %d — 재시도가 배로 늘면 실행기 큐가 터진다", round)
                        .isEqualTo(1);
                executor.drainOnce();
            }
            assertThat(clock.failed()).isTrue();
        } finally {
            campaigns.close();
        }
    }

    /**
     * 실패했던 회수가 재시도로 끝나면 tombstone 이 박혀 재등록이 막힌다.
     *
     * <p>이름을 바꿨다 — 원래는 "재시도 대기 중에 등록된 미터를 안 걷어 간다" 였는데, 이 테스트는
     * 그 창에서 등록을 시도하지 않으므로 그 계약을 검증하지 않는다. 실제로 고정하는 것은
     * <b>재시도가 끝난 뒤</b>의 재등록 차단이다.
     *
     * <p>큐잉 실행기를 쓰는 이유는 남는다 — 인라인 실행기에서는 재시도가 첫 호출 안에서
     * 재귀로 끝나 "여러 번에 걸쳐 끝난다" 는 상태 자체가 없다.
     *
     * <p>TODO(@SH-Seol · 후속 티켓): 재시도 대기 창에서 같은 회차가 재등록됐을 때 그 새 미터를
     * 걷어 가지 않는지는 아직 안 본다. {@code campaigns.remove(couponId, pending.campaignMeters())}
     * 2-인자 remove 로 막을 수 있는 자리다.
     */
    @Test
    void blocksReRegistrationOnceTheRetrySucceedsAndTombstonesTheCampaign() {
        FailingClock clock = new FailingClock(Instant.parse("2026-08-24T00:00:00Z"), 2, 1);
        QueueingScheduledExecutor executor = new QueueingScheduledExecutor();
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        CampaignMeterRegistry campaigns = new CampaignMeterRegistry(meters,
                new CampaignMeterProperties(1, Duration.ofMinutes(1), Duration.ofHours(1), 10),
                Duration.ofSeconds(10), clock, executor);
        try {
            MeterEventRecorder recorder = recorder(campaigns, meters);
            recorder.record(factory().issueAttempt(context(201L)));

            campaigns.retireCampaign(201L, Instant.parse("2026-08-23T23:59:00Z"));
            // 첫 태스크는 실패하고 하나를 다시 예약한다. 큐가 빌 때까지 돌린다.
            for (int guard = 0; guard < 10 && executor.pending() > 0; guard++) {
                executor.drainOnce();
            }
            assertThat(executor.pending()).as("재시도가 끝나야 한다").isZero();

            // 재시도가 끝나면 tombstone 이 박혀 재등록이 막혀야 한다.
            recorder.record(factory().issueAttempt(context(201L)));

            assertNoCampaignMeters(meters, "201");
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

    private static void assertNoCampaignMeters(SimpleMeterRegistry meters, String couponId) {
        assertThat(meters.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", couponId).counters()).isEmpty();
        assertThat(meters.find(MeterNames.QUEUE_ADMITTED).tag("coupon_id", couponId).counters()).isEmpty();
        assertThat(meters.find(MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH).tag("coupon_id", couponId)
                .gauges()).isEmpty();
        assertThat(meters.find(MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH).tag("coupon_id", couponId)
                .gauges()).isEmpty();
    }

    /**
     * {@code instant()} 의 {@code failFromCall} 번째 호출부터 {@code failCount} 번 던지는 시계.
     *
     * <p>{@code removeCampaignMeters} <b>바깥</b>에서 나는 예외를 만드는 유일한 seam 이다 —
     * 레지스트리의 {@code remove} 를 던지게 하면 그 메서드 안쪽 catch 에 잡혀 이 경로를 아예 안 탄다.
     *
     * <p>횟수가 파라미터인 이유 — 한 번만 실패하는 시계로는 재예약 <b>조건</b>을 검사할 수 없다.
     * 재시도가 성공해 버려서, 조건을 지운 구현도 두 번째 호출에서 재귀가 멈추기 때문이다(실측:
     * 조건을 지우고 돌려도 초록이었다). 계속 실패해야 "회수할 것이 없는데 재예약한다" 가
     * 무한 재귀로 드러난다.
     */
    private static final class FailingClock extends Clock {

        private final Instant instant;
        private final int failFromCall;
        private final int failCount;
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger failures = new AtomicInteger();

        private FailingClock(Instant instant, int failFromCall, int failCount) {
            this.instant = instant;
            this.failFromCall = failFromCall;
            this.failCount = failCount;
        }

        private boolean failed() {
            return failures.get() > 0;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            if (calls.incrementAndGet() >= failFromCall && failures.get() < failCount) {
                failures.incrementAndGet();
                throw new IllegalStateException("simulated clock failure");
            }
            return instant;
        }
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

    /**
     * 예약을 <b>큐에 담기만</b> 하는 실행기. 테스트가 명시적으로 드레인한다.
     *
     * <p>{@code ImmediateScheduledExecutor} 는 인라인 실행이라 "예약과 실행 사이" 라는 구간이
     * 존재하지 않는다. 실제 {@code ScheduledThreadPoolExecutor} 에는 그 구간이 있고, 거기서만
     * 나타나는 인터리빙이 있다.
     */
    private static final class QueueingScheduledExecutor extends AbstractExecutorService
            implements ScheduledExecutorService {

        private final java.util.Deque<Runnable> queue = new java.util.ArrayDeque<>();
        private boolean shutdown;

        private int pending() {
            return queue.size();
        }

        private void drainOnce() {
            Runnable next = queue.poll();
            if (next != null) {
                next.run();
            }
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            queue.add(command);
            return new CompletedScheduledFuture<>(null);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(
                Runnable command, long initialDelay, long period, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable command, long initialDelay, long delay, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            shutdown = true;
            queue.clear();
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
            queue.add(command);
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

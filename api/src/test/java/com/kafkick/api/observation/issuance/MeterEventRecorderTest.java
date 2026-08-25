package com.kafkick.api.observation.issuance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.kafkick.api.observation.MeterNames;
import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.ReleaseStage;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class MeterEventRecorderTest {

    @Test
    void mapsEveryEventKindToItsCampaignMeterAndOutcome() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);
        IssuanceFlowEvent.Ctx context = context(201L);

        recorder.record(factory.issueAttempt(context));
        recorder.record(factory.admitted(context, 7L));
        recorder.record(factory.issued(context, 1L, "coupon-code-0001"));
        recorder.record(factory.entry(context, 202, null, Dependency.NONE, 3L, 8L));
        recorder.record(factory.issueRejected(context, 409, ReasonCode.ALREADY_ISSUED, Dependency.NONE));

        assertThat(counter(registry, MeterNames.ISSUANCE_FLOW, "coupon_id", "201", "stage", "attempt"))
                .isEqualTo(1.0);
        assertThat(counter(registry, MeterNames.ISSUANCE_FLOW, "coupon_id", "201", "stage", "success"))
                .isEqualTo(1.0);
        assertThat(counter(registry, MeterNames.QUEUE_ADMITTED, "coupon_id", "201")).isEqualTo(1.0);
        assertThat(counter(registry, MeterNames.ISSUANCE_OUTCOME, "outcome", "ISSUED")).isEqualTo(1.0);
        assertThat(counter(registry, MeterNames.ISSUANCE_OUTCOME, "outcome", "QUEUED")).isEqualTo(1.0);
        assertThat(counter(registry, MeterNames.ISSUANCE_OUTCOME, "outcome", "ALREADY_ISSUED"))
                .isEqualTo(1.0);
        assertThat(gauge(registry, MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH, "coupon_id", "201"))
                .isEqualTo(1_787_443_200d);
        assertThat(gauge(registry, MeterNames.QUEUE_EVENT_LAST_ADMITTED_EPOCH, "coupon_id", "201"))
                .isEqualTo(1_787_443_200d);
    }

    @Test
    void preRegistersTheClosedOutcomeDictionaryWithoutCouponId() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        recorder(registry);

        // ISSUED · QUEUED 둘에 ReasonCode 12종을 더한 수다. 리터럴로 둔다 — 구현과 같은
        // ReasonCode.values().length 를 쓰면 사유가 늘 때 이 테스트가 조용히 따라가고, 사유를
        // 더한 사람이 미터 사전을 봤는지 아무도 묻지 않게 된다. 여기서 갈리는 것이 그 질문이다.
        assertThat(registry.find(MeterNames.ISSUANCE_OUTCOME).counters()).hasSize(14);
        assertThat(registry.find(MeterNames.ISSUANCE_OUTCOME).counters())
                .allMatch(counter -> counter.getId().getTag("coupon_id") == null);
        assertThat(registry.find(MeterNames.ISSUANCE_OUTCOME).counters().stream()
                .map(counter -> counter.getId().getTag("outcome"))
                .collect(Collectors.toSet()))
                .isEqualTo(Stream.concat(
                        Stream.of("ISSUED", "QUEUED"),
                        Stream.of(ReasonCode.values()).map(Enum::name)
                ).collect(Collectors.toSet()));
        for (ReasonCode reasonCode : ReasonCode.values()) {
            assertThat(registry.find(MeterNames.ISSUANCE_OUTCOME)
                    .tag("outcome", reasonCode.name()).counter()).isNotNull();
        }
    }

    @Test
    void reusesOneRegistrationPathAndReportsNoEventEpochAsNaN() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);

        recorder.record(factory.issueAttempt(context(201L)));
        recorder.record(factory.issueAttempt(context(201L)));

        assertThat(registry.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters())
                .hasSize(2);
        assertThat(gauge(registry, MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH, "coupon_id", "201"))
                .isNaN();
    }

    @Test
    void neverMovesLastEventEpochBackwardWhenEventsArriveOutOfOrder() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);

        recorder.record(factory.issued(context(201L, "2026-08-23T00:01:00Z"), 1L,
                "coupon-code-0001"));
        recorder.record(factory.issued(context(201L, "2026-08-23T00:00:00Z"), 2L,
                "coupon-code-0002"));

        assertThat(gauge(registry, MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH, "coupon_id", "201"))
                .isEqualTo(1_787_443_260d);
    }

    @Test
    void doesNotCountAReplayedIssueResultAsANewIssuance() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);
        recorder.record(factory.issueAttempt(context(201L)));

        recorder.record(factory.issued(context(201L, true), 1L, "coupon-code-0001"));

        assertThat(counter(registry, MeterNames.ISSUANCE_FLOW,
                "coupon_id", "201", "stage", "success")).isZero();
        assertThat(counter(registry, MeterNames.ISSUANCE_OUTCOME, "outcome", "ISSUED")).isZero();
    }

    @Test
    void countsReplayedIssueAttemptsAsRepeatedEngineEntries() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);

        recorder.record(factory.issueAttempt(context(201L, true)));

        assertThat(counter(registry, MeterNames.ISSUANCE_FLOW,
                "coupon_id", "201", "stage", "attempt")).isEqualTo(1.0);
    }

    @Test
    void countsReplayedRejectionsButNotReplayedQueueAdmissions() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);
        IssuanceFlowEvent.Ctx replayed = context(201L, true);

        recorder.record(factory.issueRejected(replayed, 409, ReasonCode.ALREADY_ISSUED,
                Dependency.NONE));
        recorder.record(factory.entry(replayed, 202, null, Dependency.NONE, 3L, 8L));

        assertThat(counter(registry, MeterNames.ISSUANCE_OUTCOME,
                "outcome", "ALREADY_ISSUED")).isEqualTo(1.0);
        assertThat(counter(registry, MeterNames.ISSUANCE_OUTCOME, "outcome", "QUEUED")).isZero();
    }

    @Test
    void keepsImmediateAdmissionOutOfTheOutcomeDictionary() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);

        recorder.record(factory.entry(context(201L), 200, null, Dependency.NONE, null, null));

        assertThat(registry.find(MeterNames.ISSUANCE_OUTCOME).counters())
                .allMatch(counter -> counter.count() == 0.0);
        assertThat(registry.find(MeterNames.ISSUANCE_FLOW).counters()).isEmpty();
    }

    @Test
    void rejectsNonPositiveFailureLogIntervals() {
        assertThatThrownBy(() -> new MeterEventRecorder(
                new CampaignMeterRegistry(new SimpleMeterRegistry(),
                        new CampaignMeterProperties(null, null, null, null), Duration.ofSeconds(1)), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("failureLogInterval must be positive");
    }

    @Test
    void registersCampaignMetersOnlyOnceUnderConcurrentFirstEvents() throws Exception {
        CountingSimpleMeterRegistry registry = new CountingSimpleMeterRegistry();
        MeterEventRecorder recorder = recorder(registry);
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(java.util.UUID::randomUUID);
        int countersBeforeCampaignRegistration = registry.counterCreations.get();
        int gaugesBeforeCampaignRegistration = registry.gaugeCreations.get();
        // 스레드를 순차 소비하면 대부분 등록이 끝난 뒤 실행되어 최초 등록 경합 창이 닫힌다.
        // 32개를 모두 띄워 놓고 한 번에 발사해야 campaignMeters 진입이 실제로 겹친다.
        int concurrency = 32;
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch fire = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(concurrency);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int index = 0; index < concurrency; index++) {
                tasks.add(() -> {
                    ready.countDown();
                    if (!fire.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("발사 신호가 오지 않았다");
                    }
                    recorder.record(factory.issueAttempt(context(201L)));
                    return null;
                });
            }
            List<java.util.concurrent.Future<Void>> futures = new ArrayList<>();
            tasks.forEach(task -> futures.add(executor.submit(task)));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            fire.countDown();
            futures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception exception) {
                    throw new AssertionError(exception);
                }
            });
        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertThat(registry.find(MeterNames.ISSUANCE_FLOW).tag("coupon_id", "201").counters())
                .hasSize(2);
        assertThat(counter(registry, MeterNames.ISSUANCE_FLOW,
                "coupon_id", "201", "stage", "attempt")).isEqualTo(32.0);
        assertThat(registry.counterCreations).hasValue(countersBeforeCampaignRegistration + 3);
        assertThat(registry.gaugeCreations).hasValue(gaugesBeforeCampaignRegistration + 2);

        // 미터 개수만 세면 등록이 겹쳐도 통과한다 — Micrometer 가 같은 id 를 합쳐 주기 때문이다.
        // 겹치면 갈라지는 것은 gauge 가 읽는 홀더다. 등록이 두 번 일어나면 맵에 남은 CampaignMeters
        // 의 홀더와 registry 가 붙든 홀더가 달라져, 성공을 아무리 기록해도 gauge 는 NaN 에 멈춘다.
        recorder.record(factory.issued(context(201L), 1L, "coupon-code-0001"));
        assertThat(gauge(registry, MeterNames.ISSUANCE_EVENT_LAST_SUCCESS_EPOCH, "coupon_id", "201"))
                .isEqualTo(1_787_443_200d);
    }

    @Test
    void observationFailureDoesNotEscapeTheIssuancePathAndIsRateLimited(CapturedOutput output) {
        MeterEventRecorder recorder = recorder(new SimpleMeterRegistry());

        assertThatCode(() -> recorder.record(null)).doesNotThrowAnyException();
        assertThatCode(() -> recorder.record(null)).doesNotThrowAnyException();
        assertThat(output).contains("캠페인 발급 미터 기록에 실패했습니다")
                .contains("누적 1건");
    }

    private static double counter(SimpleMeterRegistry registry, String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        assertThat(counter).isNotNull();
        return counter.count();
    }

    private static double gauge(SimpleMeterRegistry registry, String name, String... tags) {
        Gauge gauge = registry.find(name).tags(tags).gauge();
        assertThat(gauge).isNotNull();
        return gauge.value();
    }

    private static IssuanceFlowEvent.Ctx context(long couponId) {
        return context(couponId, false);
    }

    private static MeterEventRecorder recorder(io.micrometer.core.instrument.MeterRegistry registry) {
        return new MeterEventRecorder(new CampaignMeterRegistry(registry,
                new CampaignMeterProperties(null, null, null, null), Duration.ofSeconds(10)),
                Duration.ofSeconds(10));
    }

    private static IssuanceFlowEvent.Ctx context(long couponId, boolean replayed) {
        return context(couponId, replayed, "2026-08-23T00:00:00Z");
    }

    private static IssuanceFlowEvent.Ctx context(long couponId, String occurredAt) {
        return context(couponId, false, occurredAt);
    }

    private static IssuanceFlowEvent.Ctx context(long couponId, boolean replayed, String occurredAt) {
        return new IssuanceFlowEvent.Ctx(
                "request-1", 101L, couponId, Grade.GOLD, replayed,
                Instant.parse(occurredAt), EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, 901L, "api-1"
        );
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
        protected <T> Gauge newGauge(Meter.Id id, T obj, ToDoubleFunction<T> valueFunction) {
            gaugeCreations.incrementAndGet();
            return super.newGauge(id, obj, valueFunction);
        }
    }
}

package com.kafkick.api.observation.issuance;

import com.kafkick.core.member.Grade;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.EventRecorder;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.IssuanceFlowEvent;
import com.kafkick.core.observation.IssuanceFlowEventFactory;
import com.kafkick.core.observation.QueueMode;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.observation.ReleaseStage;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.ErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(OutputCaptureExtension.class)
class IssuanceObservationSessionTest {

    private static final UUID EVENT_ID =
            UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final Instant COMPLETED_AT = Instant.parse("2026-08-19T01:00:05Z");
    private static final TimeProvider TIME_PROVIDER = new TimeProvider(
            Clock.fixed(COMPLETED_AT, ZoneOffset.UTC)
    );

    @Test
    void recordsIssuedResultWithIssuanceIdentifiers() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.completeIssued(301L, "ISSUE-CODE-301");
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.eventId()).isEqualTo(EVENT_ID);
            assertThat(event.eventType()).isEqualTo(EventType.ISSUE_RESULT);
            assertThat(event.httpStatus()).isEqualTo(201);
            assertThat(event.issuanceId()).isEqualTo(301L);
            assertThat(event.issuanceCode()).isEqualTo("ISSUE-CODE-301");
            assertThat(event.reasonCode()).isNull();
            assertThat(event.dependency()).isEqualTo(Dependency.NONE);
            assertThat(event.occurredAt()).isEqualTo(COMPLETED_AT);
        });
    }

    @Test
    void recordsRejectedResultWithMappedReasonCode() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.completeIssueRejected(TestErrorCode.ALREADY_ISSUED, Dependency.NONE);
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.eventType()).isEqualTo(EventType.ISSUE_RESULT);
            assertThat(event.httpStatus()).isEqualTo(409);
            assertThat(event.reasonCode()).isEqualTo(ReasonCode.ALREADY_ISSUED);
            assertThat(event.dependency()).isEqualTo(Dependency.NONE);
            assertThat(event.issuanceId()).isNull();
            assertThat(event.issuanceCode()).isNull();
        });
    }

    @Test
    void recordsUnmappedReasonCodeWhenErrorCodeHasNoMapping() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.completeIssueRejected(TestErrorCode.UNMAPPED, Dependency.MYSQL);
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.httpStatus()).isEqualTo(409);
            assertThat(event.reasonCode()).isEqualTo(ReasonCode.UNMAPPED);
            assertThat(event.dependency()).isEqualTo(Dependency.MYSQL);
        });
    }

    @Test
    void recordsImmediatelyAdmittedEntryWithoutQueueInformation() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.completeEntryAdmitted();
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.eventType()).isEqualTo(EventType.ENTRY_RESULT);
            assertThat(event.httpStatus()).isEqualTo(200);
            assertThat(event.reasonCode()).isNull();
            assertThat(event.queuePosition()).isNull();
            assertThat(event.queueSequence()).isNull();
            assertThat(event.occurredAt()).isEqualTo(COMPLETED_AT);
        });
    }

    @Test
    void recordsQueuedEntryWithQueueInformation() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.completeEntryQueued(17L, 41L);
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.eventType()).isEqualTo(EventType.ENTRY_RESULT);
            assertThat(event.httpStatus()).isEqualTo(202);
            assertThat(event.reasonCode()).isNull();
            assertThat(event.queuePosition()).isEqualTo(17L);
            assertThat(event.queueSequence()).isEqualTo(41L);
            assertThat(event.occurredAt()).isEqualTo(COMPLETED_AT);
        });
    }

    @Test
    void recordsRejectedEntryWithMappedReasonCode() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.completeEntryRejected(
                TestErrorCode.ALREADY_ISSUED,
                Dependency.NONE
        );
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.eventType()).isEqualTo(EventType.ENTRY_RESULT);
            assertThat(event.httpStatus()).isEqualTo(409);
            assertThat(event.reasonCode()).isEqualTo(ReasonCode.ALREADY_ISSUED);
            assertThat(event.queuePosition()).isNull();
            assertThat(event.queueSequence()).isNull();
        });
    }

    @Test
    void keepsTheFirstCompletedResult() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.completeIssued(301L, "ISSUE-CODE-301");
        session.completeIssueRejected(TestErrorCode.ALREADY_ISSUED, Dependency.NONE);
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.httpStatus()).isEqualTo(201);
            assertThat(event.issuanceId()).isEqualTo(301L);
        });
    }

    @Test
    void keepsTheFirstCompletedResultTimeWhenFinishRunsLater() {
        Instant secondCompletionAt = Instant.parse("2026-08-19T01:00:06Z");
        Instant finishedAt = Instant.parse("2026-08-19T01:00:10Z");
        MutableClock clock = new MutableClock(COMPLETED_AT);
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationService service = service(
                recordedEvents::add,
                new TimeProvider(clock)
        );
        IssuanceObservationSession session = service.begin(context());

        session.completeIssued(301L, "ISSUE-CODE-301");
        clock.setInstant(secondCompletionAt);
        session.completeIssueRejected(TestErrorCode.ALREADY_ISSUED, Dependency.NONE);
        clock.setInstant(finishedAt);
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.httpStatus()).isEqualTo(201);
            assertThat(event.occurredAt()).isEqualTo(COMPLETED_AT);
        });
    }

    @Test
    void recordsAtMostOnceWhenFinishIsCalledRepeatedly() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);
        session.completeIssued(301L, "ISSUE-CODE-301");

        session.finish();
        session.finish();
        session.finish();

        assertThat(recordedEvents).hasSize(1);
    }

    @Test
    void skipsRecordingWhenNoResultWasCompleted() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.finish();

        assertThat(recordedEvents).isEmpty();
    }

    @Test
    void ignoresAResultCompletedAfterFinish() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        session.finish();
        session.completeIssued(301L, "ISSUE-CODE-301");
        session.finish();

        assertThat(recordedEvents).isEmpty();
    }

    @Test
    void isolatesEventCreationFailureFromTheCaller() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);
        session.completeIssued(301L, "TOO-LONG-ISSUANCE-CODE");

        assertThatCode(session::finish).doesNotThrowAnyException();
        assertThat(recordedEvents).isEmpty();
    }

    @Test
    void logsWarningWithRequestAndEventTypeWhenRecordingFails(CapturedOutput output) {
        IssuanceObservationSession session = session(new CopyOnWriteArrayList<>());
        session.completeIssued(301L, "TOO-LONG-ISSUANCE-CODE");

        session.finish();

        assertThat(output)
                .contains("WARN")
                .contains("requestId=request-1")
                .contains("eventType=ISSUE_RESULT");
    }

    @Test
    void isolatesRecorderFailureFromTheCaller() {
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(() -> EVENT_ID);
        IssuanceObservationService service = new IssuanceObservationService(
                factory,
                event -> {
                    throw new IllegalStateException("recorder unavailable");
                },
                TIME_PROVIDER
        );
        IssuanceObservationSession session = service.begin(context());
        session.completeIssued(301L, "ISSUE-CODE-301");

        assertThatCode(session::finish).doesNotThrowAnyException();
    }

    @Test
    void doesNotRetryAfterRecorderFailure() {
        AtomicInteger attempts = new AtomicInteger();
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(() -> EVENT_ID);
        IssuanceObservationService service = new IssuanceObservationService(
                factory,
                event -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("recorder unavailable");
                },
                TIME_PROVIDER
        );
        IssuanceObservationSession session = service.begin(context());
        session.completeIssued(301L, "ISSUE-CODE-301");

        session.finish();
        session.finish();

        assertThat(attempts).hasValue(1);
    }

    @Test
    void doesNotCatchErrorsFromRecorder() {
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(() -> EVENT_ID);
        IssuanceObservationService service = new IssuanceObservationService(
                factory,
                event -> {
                    throw new AssertionError("fatal recorder error");
                },
                TIME_PROVIDER
        );
        IssuanceObservationSession session = service.begin(context());
        session.completeIssued(301L, "ISSUE-CODE-301");

        assertThatThrownBy(session::finish)
                .isInstanceOf(AssertionError.class)
                .hasMessage("fatal recorder error");
    }

    @Test
    void acceptsOnlyOneResultWhenCompletionsRace() throws Exception {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        runConcurrently(20, index -> {
            if (index % 2 == 0) {
                session.completeIssued(301L, "ISSUE-CODE-301");
            } else {
                session.completeIssueRejected(TestErrorCode.ALREADY_ISSUED, Dependency.NONE);
            }
        });
        session.finish();

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.httpStatus()).isIn(201, 409);
            if (event.httpStatus() == 201) {
                assertThat(event.issuanceId()).isEqualTo(301L);
                assertThat(event.reasonCode()).isNull();
            } else {
                assertThat(event.issuanceId()).isNull();
                assertThat(event.reasonCode()).isEqualTo(ReasonCode.ALREADY_ISSUED);
            }
        });
    }

    @Test
    void givesOnlyOneCallerRecordingResponsibilityWhenFinishesRace() throws Exception {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);
        session.completeIssued(301L, "ISSUE-CODE-301");

        runConcurrently(20, ignored -> session.finish());

        assertThat(recordedEvents).hasSize(1);
    }

    @Test
    void recordsAtMostOnceWhenCompletionAndFinishRace() throws Exception {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationSession session = session(recordedEvents);

        runConcurrently(20, index -> {
            if (index % 2 == 0) {
                session.completeIssued(301L, "ISSUE-CODE-301");
            } else {
                session.finish();
            }
        });

        assertThat(recordedEvents).hasSizeLessThanOrEqualTo(1);
    }

    @Test
    void recordsQueueAdmittedThroughIndependentFailSafeBoundary() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationService service = service(recordedEvents::add);

        service.recordAdmitted(context(), 41L);

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.eventType()).isEqualTo(EventType.QUEUE_ADMITTED);
            assertThat(event.queueSequence()).isEqualTo(41L);
            assertThat(event.httpStatus()).isNull();
            assertThat(event.occurredAt()).isEqualTo(COMPLETED_AT);
        });
    }

    @Test
    void isolatesQueueAdmittedRecorderFailureFromTheCaller() {
        IssuanceObservationService service = service(event -> {
            throw new IllegalStateException("recorder unavailable");
        });

        assertThatCode(() -> service.recordAdmitted(context(), 41L))
                .doesNotThrowAnyException();
    }

    @Test
    void isolatesQueueAdmittedTimeFailureFromTheCaller() {
        Clock unavailableClock = new Clock() {
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
                throw new IllegalStateException("clock unavailable");
            }
        };
        IssuanceObservationService service = service(
                event -> { },
                new TimeProvider(unavailableClock)
        );

        assertThatCode(() -> service.recordAdmitted(context(), 41L))
                .doesNotThrowAnyException();
    }

    @Test
    void isolatesInvalidQueueAdmittedEventFromTheCaller() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationService service = service(recordedEvents::add);

        assertThatCode(() -> service.recordAdmitted(context(), -1L))
                .doesNotThrowAnyException();
        assertThat(recordedEvents).isEmpty();
    }

    @Test
    void rejectsNullContextWhenRecordingQueueAdmission() {
        IssuanceObservationService service = service(event -> { });

        assertThatThrownBy(() -> service.recordAdmitted(null, 41L))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("context");
    }

    @Test
    void recordsIssueAttemptThroughIndependentFailSafeBoundary() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationService service = service(recordedEvents::add);

        service.recordIssueAttempt(context());

        assertThat(recordedEvents).singleElement().satisfies(event -> {
            assertThat(event.eventType()).isEqualTo(EventType.ISSUE_ATTEMPT);
            assertThat(event.httpStatus()).isNull();
            assertThat(event.queueSequence()).isNull();
            assertThat(event.occurredAt()).isEqualTo(COMPLETED_AT);
        });
    }

    @Test
    void keepsSessionResultAfterIssueAttemptIsRecorded() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationService service = service(recordedEvents::add);
        IssuanceObservationSession session = service.begin(context());

        service.recordIssueAttempt(context());
        session.completeIssued(301L, "ISSUE-CODE-301");
        session.finish();

        assertThat(recordedEvents).extracting(IssuanceFlowEvent::eventType)
                .containsExactly(EventType.ISSUE_ATTEMPT, EventType.ISSUE_RESULT);
    }

    @Test
    void isolatesIssueAttemptRecorderFailureFromTheCaller() {
        IssuanceObservationService service = service(event -> {
            throw new IllegalStateException("recorder unavailable");
        });

        assertThatCode(() -> service.recordIssueAttempt(context()))
                .doesNotThrowAnyException();
    }

    @Test
    void isolatesInvalidIssueAttemptEventFromTheCaller() {
        List<IssuanceFlowEvent> recordedEvents = new CopyOnWriteArrayList<>();
        IssuanceObservationService service = service(recordedEvents::add);

        assertThatCode(() -> service.recordIssueAttempt(contextWithoutRequestId()))
                .doesNotThrowAnyException();
        assertThat(recordedEvents).isEmpty();
    }

    @Test
    void logsWarningWithRequestIdWhenIssueAttemptRecordingFails(CapturedOutput output) {
        IssuanceObservationService service = service(event -> {
            throw new IllegalStateException("recorder unavailable");
        });

        service.recordIssueAttempt(context());

        assertThat(output).contains("WARN");
        assertThat(output).contains("requestId=request-1");
        assertThat(output).contains("eventType=ISSUE_ATTEMPT");
        assertThat(output).contains("IllegalStateException: recorder unavailable");
    }

    @Test
    void logsIssueAttemptFailureAtMostOncePerInterval(CapturedOutput output) {
        IssuanceObservationService service = service(event -> {
            throw new IllegalStateException("recorder unavailable");
        });

        for (int attempt = 0; attempt < 50; attempt++) {
            service.recordIssueAttempt(context());
        }

        // 계약 위반은 요청마다 재발한다. 20,000 RPS 스파이크에서 건당 WARN 은 동기 appender 가
        // 요청 스레드를 붙잡아 측정 자체를 오염시킨다.
        assertThat(countOccurrences(output.toString(), "발급 시도 관측 이벤트 기록에 실패했습니다"))
                .isEqualTo(1);
    }

    @Test
    void logsIssueAttemptFailureAgainAfterIntervalElapses(CapturedOutput output) {
        AtomicLong nanos = new AtomicLong();
        IssuanceObservationService service = new IssuanceObservationService(
                new IssuanceFlowEventFactory(() -> EVENT_ID),
                event -> {
                    throw new IllegalStateException("recorder unavailable");
                },
                TIME_PROVIDER,
                nanos::get
        );

        service.recordIssueAttempt(context());
        service.recordIssueAttempt(context());
        nanos.addAndGet(TimeUnit.SECONDS.toNanos(10));
        service.recordIssueAttempt(context());

        // 조인 것이 영구 침묵이 되면 안 된다. 창이 다시 열리고 누적 건수가 실린다.
        assertThat(countOccurrences(output.toString(), "발급 시도 관측 이벤트 기록에 실패했습니다"))
                .isEqualTo(2);
        assertThat(output).contains("누적 3건");
    }

    @Test
    void keepsIssueAttemptFailureLogThrottledUnderConcurrentFailures(CapturedOutput output)
            throws Exception {
        // 시계를 세워 둔다. 흐르는 시계면 첫 스레드가 창을 밀어 놓은 뒤 도착한 스레드가
        // 시각 비교에서 먼저 걸러진다.
        //
        // 이 테스트가 보장하는 것은 동시 실패에서도 창이 한 번만 열린다는 것까지다.
        // CAS 를 set 으로 퇴화시키면 20회 중 9회만 빨간불이 난다(실측) — 두 스레드가 같은 due 를
        // 읽는 순간을 강제할 수 없어서다. CAS 삭제 회귀를 CI 1회 실행이 놓칠 확률이 55% 이므로
        // 이 테스트를 CAS 검증으로 읽으면 안 된다.
        IssuanceObservationService service = new IssuanceObservationService(
                new IssuanceFlowEventFactory(() -> EVENT_ID),
                event -> {
                    throw new IllegalStateException("recorder unavailable");
                },
                TIME_PROVIDER,
                () -> 0L
        );

        runConcurrently(20, index -> service.recordIssueAttempt(context()));

        // 창을 잡은 스레드가 그 시점의 누적을 싣는다. 20 이 아니라 1~20 중 하나다 —
        // 로그가 세는 것은 억제된 건수가 아니라 누적 건수다.
        assertThat(countOccurrences(output.toString(), "발급 시도 관측 이벤트 기록에 실패했습니다"))
                .isEqualTo(1);
        assertThat(output.toString()).containsPattern("누적 (?:[1-9]|1[0-9]|20)건");
    }

    @Test
    void doesNotFormatStackTraceWhenIssueAttemptRecordingFails(CapturedOutput output) {
        IssuanceObservationService service = service(event -> {
            throw new IllegalStateException("recorder unavailable");
        });

        service.recordIssueAttempt(context());

        // 발급 요청 스레드가 포맷 비용을 문다. 계약 위반은 요청마다 결정론적으로 재발한다.
        assertThat(output).doesNotContain("at com.kafkick.api.observation");
    }

    @Test
    void rejectsNullContextWhenRecordingIssueAttempt() {
        IssuanceObservationService service = service(event -> { });

        assertThatThrownBy(() -> service.recordIssueAttempt(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("context");
    }

    private static IssuanceObservationSession session(List<IssuanceFlowEvent> recordedEvents) {
        return service(recordedEvents::add).begin(context());
    }

    private static IssuanceObservationService service(EventRecorder eventRecorder) {
        return service(eventRecorder, TIME_PROVIDER);
    }

    private static IssuanceObservationService service(
            EventRecorder eventRecorder,
            TimeProvider timeProvider
    ) {
        IssuanceFlowEventFactory factory = new IssuanceFlowEventFactory(() -> EVENT_ID);
        return new IssuanceObservationService(factory, eventRecorder, timeProvider);
    }

    private static IssuanceFlowEvent.Ctx context() {
        return new IssuanceFlowEvent.Ctx(
                "request-1",
                101L,
                201L,
                Grade.GOLD,
                false,
                Instant.parse("2026-08-19T01:00:00Z"),
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                901L,
                "api-1"
        );
    }

    private static int countOccurrences(String text, String token) {
        int count = 0;
        for (int index = text.indexOf(token); index >= 0; index = text.indexOf(token, index + 1)) {
            count++;
        }
        return count;
    }

    private static IssuanceFlowEvent.Ctx contextWithoutRequestId() {
        IssuanceFlowEvent.Ctx context = context();
        return new IssuanceFlowEvent.Ctx(
                null,
                context.memberId(),
                context.couponId(),
                context.grade(),
                context.replayed(),
                context.occurredAt(),
                context.engineVersion(),
                context.releaseStage(),
                context.queueMode(),
                context.benchmarkRunId(),
                context.producerInstanceId()
        );
    }

    private static void runConcurrently(int taskCount, IndexedTask task) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(taskCount);
        CountDownLatch ready = new CountDownLatch(taskCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>(taskCount);
        try {
            for (int index = 0; index < taskCount; index++) {
                int taskIndex = index;
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        task.run(taskIndex);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("동시성 테스트 시작 대기가 중단됐습니다.", exception);
                    }
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface IndexedTask {

        void run(int index);
    }

    private static final class MutableClock extends Clock {

        private final AtomicReference<Instant> currentInstant;

        private MutableClock(Instant initialInstant) {
            this.currentInstant = new AtomicReference<>(initialInstant);
        }

        private void setInstant(Instant instant) {
            currentInstant.set(instant);
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
            return currentInstant.get();
        }
    }

    private enum TestErrorCode implements ErrorCode {

        ALREADY_ISSUED(Optional.of(ReasonCode.ALREADY_ISSUED)),
        UNMAPPED(Optional.empty());

        private final Optional<ReasonCode> reasonCode;

        TestErrorCode(Optional<ReasonCode> reasonCode) {
            this.reasonCode = reasonCode;
        }

        @Override
        public int getStatus() {
            return 409;
        }

        @Override
        public String getCode() {
            return name();
        }

        @Override
        public String getMessage() {
            return name();
        }

        @Override
        public Optional<ReasonCode> reasonCode() {
            return reasonCode;
        }
    }
}

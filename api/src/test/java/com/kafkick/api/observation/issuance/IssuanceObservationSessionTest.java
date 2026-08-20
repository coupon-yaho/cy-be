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

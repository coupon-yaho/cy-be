package com.kafkick.core.observation;

import com.kafkick.core.member.Grade;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IssuanceFlowEventTest {

    private static final UUID EVENT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final Instant OCCURRED_AT = Instant.parse("2026-08-15T05:02:31.120Z");
    private static final IssuanceFlowEventFactory FACTORY =
            new IssuanceFlowEventFactory(() -> EVENT_ID);

    @Test
    void admittedFactoryCannotReceiveIssuanceFields() {
        IssuanceFlowEvent event = FACTORY.admitted(context(null, false), 18L);

        assertThat(event.eventType()).isEqualTo(EventType.QUEUE_ADMITTED);
        assertThat(event.httpStatus()).isNull();
        assertThat(event.issuanceId()).isNull();
        assertThat(event.issuanceCode()).isNull();
        assertThat(event.queuePosition()).isNull();
        assertThat(event.queueSequence()).isEqualTo(18L);
        assertThat(event.dependency()).isEqualTo(Dependency.NONE);
        assertThat(event.replayed()).isFalse();
    }

    @Test
    void issueAttemptFactoryCarriesOnlyStageFields() {
        IssuanceFlowEvent event = FACTORY.issueAttempt(context("request-1", false));

        assertThat(event.eventType()).isEqualTo(EventType.ISSUE_ATTEMPT);
        assertThat(event.schemaVersion()).isEqualTo(IssuanceFlowEvent.CURRENT_SCHEMA_VERSION);
        assertThat(event.requestId()).isEqualTo("request-1");
        assertThat(event.memberId()).isEqualTo(7L);
        assertThat(event.couponId()).isEqualTo(9L);
        assertThat(event.dependency()).isEqualTo(Dependency.NONE);
        assertThat(event.httpStatus()).isNull();
        assertThat(event.reasonCode()).isNull();
        assertThat(event.issuanceId()).isNull();
        assertThat(event.issuanceCode()).isNull();
        assertThat(event.queuePosition()).isNull();
        assertThat(event.queueSequence()).isNull();
    }

    @Test
    void issueAttemptKeepsReplayedFlagFromContext() {
        IssuanceFlowEvent event = FACTORY.issueAttempt(context("request-1", true));

        assertThat(event.replayed()).isTrue();
    }

    @Test
    void issueAttemptRequiresRequestId() {
        assertThatThrownBy(() -> FACTORY.issueAttempt(context(null, false)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FACTORY.issueAttempt(context("x".repeat(37), false)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsResultFieldsOnIssueAttempt() {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_ATTEMPT, 201, null, null, null, null, null
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ISSUE_ATTEMPT 필드 계약을 위반했습니다.");

        assertThatThrownBy(() -> event(
                EventType.ISSUE_ATTEMPT, null, 101L, "ISSUANCE0000001", null, null, null
        )).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> event(
                EventType.ISSUE_ATTEMPT, null, null, null,
                ReasonCode.STOCK_EXHAUSTED, null, null
        )).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> event(
                EventType.ISSUE_ATTEMPT, null, null, null, null, 12L, 18L
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsDependencyOnIssueAttempt() {
        assertThatThrownBy(() -> new IssuanceFlowEvent(
                1, EVENT_ID, EventType.ISSUE_ATTEMPT, "request-1", 7L, 9L,
                null, null, Grade.GOLD, null, null, Dependency.REDIS,
                null, null, false, OCCURRED_AT, EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, null, "api-1"
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("ISSUE_ATTEMPT 필드 계약을 위반했습니다.");
    }

    @Test
    void admittedFactoryRejectsReplayedContext() {
        assertThatThrownBy(() -> FACTORY.admitted(context(null, true), 18L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsIssuanceFieldsOnAdmittedEvent() {
        assertThatThrownBy(() -> new IssuanceFlowEvent(
                1, EVENT_ID, EventType.QUEUE_ADMITTED, null, 7L, 9L,
                10L, "ISSUANCE0000001", Grade.GOLD, null, null, Dependency.NONE,
                null, 18L, false, OCCURRED_AT, EngineVersion.V3, ReleaseStage.V3,
                QueueMode.ADAPTIVE, null, "api-1"
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsNegativeQueueSequence() {
        assertThatThrownBy(() -> event(
                EventType.ENTRY_RESULT, 202, null, null,
                null, 0L, -1L
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsNonPositiveQueuePosition() {
        assertThatThrownBy(() -> event(
                EventType.ENTRY_RESULT, 202, null, null,
                null, 0L, 10L
        )).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> event(
                EventType.ENTRY_RESULT, 202, null, null,
                null, -1L, 10L
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorAcceptsMinimumQueuePosition() {
        IssuanceFlowEvent entry = event(
                EventType.ENTRY_RESULT, 202, null, null,
                null, 1L, 0L
        );

        assertThat(entry.queuePosition()).isEqualTo(1L);
    }

    @Test
    void canonicalConstructorRejectsNonPositiveIssuanceId() {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_RESULT, 201, 0L, "ISSUANCE0000001",
                null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsBlankIssuanceCode() {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_RESULT, 201, 1L, " ",
                null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorRejectsOverlongIssuanceCode() {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_RESULT, 201, 1L, "12345678901234567",
                null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void canonicalConstructorAcceptsZeroQueueSequence() {
        IssuanceFlowEvent admitted = event(
                EventType.ENTRY_RESULT, 202, null, null,
                null, 1L, 0L
        );

        assertThat(admitted.queueSequence()).isZero();
    }

    @Test
    void canonicalConstructorAcceptsMinimumIssuanceIdentity() {
        IssuanceFlowEvent issued = event(
                EventType.ISSUE_RESULT, 201, 1L, "A",
                null, null, null
        );

        assertThat(issued.issuanceId()).isEqualTo(1L);
        assertThat(issued.issuanceCode()).isEqualTo("A");
    }

    @Test
    void contextCopiesAllFieldsAndReplacesOnlyOccurredAt() {
        IssuanceFlowEvent.Ctx original = context("request-1", false);
        Instant completedAt = Instant.parse("2026-08-15T05:02:35.120Z");

        IssuanceFlowEvent.Ctx completed = original.withOccurredAt(completedAt);

        assertThat(completed).isEqualTo(new IssuanceFlowEvent.Ctx(
                original.requestId(),
                original.memberId(),
                original.couponId(),
                original.grade(),
                original.replayed(),
                completedAt,
                original.engineVersion(),
                original.releaseStage(),
                original.queueMode(),
                original.benchmarkRunId(),
                original.producerInstanceId()
        ));
    }

    @Test
    void failedHttpResultRequiresReasonCode() {
        assertThatThrownBy(() -> FACTORY.issueRejected(
                context("request-1", false), 409, null, Dependency.NONE
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void failedResultCanBeRecordedBeforeGradeIsKnown() {
        IssuanceFlowEvent.Ctx context = context("request-1", false, null);

        IssuanceFlowEvent event = FACTORY.issueRejected(
                context, 503, ReasonCode.TEMPORARILY_UNAVAILABLE, Dependency.MYSQL
        );

        assertThat(event.grade()).isNull();
    }

    @Test
    void rejectsIssuanceCodeWhenStatusIsNotCreated() {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_RESULT, 409, 101L, "ISSUANCE0000001",
                ReasonCode.STOCK_EXHAUSTED, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsCreatedResultWithoutIssuanceId() {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_RESULT, 201, null, "ISSUANCE0000001",
                null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAcceptedEntryWithoutQueuePosition() {
        assertThatThrownBy(() -> event(
                EventType.ENTRY_RESULT, 202, null, null,
                null, null, 18L
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsQueuePositionWhenEntryIsNotAccepted() {
        assertThatThrownBy(() -> event(
                EventType.ENTRY_RESULT, 200, null, null,
                null, 12L, 18L
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {199, 201, 204, 300, 302, 399})
    void rejectsEntrySuccessStatusOtherThanOkOrAccepted(int invalidStatus) {
        assertThatThrownBy(() -> event(
                EventType.ENTRY_RESULT, invalidStatus, null, null,
                null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {199, 200, 202, 204, 300, 302, 399})
    void rejectsIssueSuccessStatusOtherThanCreated(int invalidStatus) {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_RESULT, invalidStatus, null, null,
                null, null, null
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("성공 ISSUE_RESULT의 HTTP 상태는 201이어야 합니다.");
    }

    @Test
    void rejectsIssuanceFieldsOnEntryResult() {
        assertThatThrownBy(() -> event(
                EventType.ENTRY_RESULT, 200, 101L, "ISSUANCE0000001",
                null, null, null
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsQueueFieldsOnIssueResult() {
        assertThatThrownBy(() -> event(
                EventType.ISSUE_RESULT, 201, 101L, "ISSUANCE0000001",
                null, 12L, 18L
        )).isInstanceOf(IllegalArgumentException.class);
    }

    static IssuanceFlowEvent.Ctx context(String requestId, boolean replayed) {
        return context(requestId, replayed, Grade.GOLD);
    }

    static IssuanceFlowEvent.Ctx context(String requestId, boolean replayed, Grade grade) {
        return new IssuanceFlowEvent.Ctx(
                requestId,
                7L,
                9L,
                grade,
                replayed,
                OCCURRED_AT,
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                42L,
                "api-1"
        );
    }

    private static IssuanceFlowEvent event(
            EventType eventType,
            Integer httpStatus,
            Long issuanceId,
            String issuanceCode,
            ReasonCode reasonCode,
            Long queuePosition,
            Long queueSequence
    ) {
        return new IssuanceFlowEvent(
                1,
                EVENT_ID,
                eventType,
                "request-1",
                7L,
                9L,
                issuanceId,
                issuanceCode,
                Grade.GOLD,
                httpStatus,
                reasonCode,
                Dependency.NONE,
                queuePosition,
                queueSequence,
                false,
                OCCURRED_AT,
                EngineVersion.V3,
                ReleaseStage.V3,
                QueueMode.ADAPTIVE,
                42L,
                "api-1"
        );
    }
}

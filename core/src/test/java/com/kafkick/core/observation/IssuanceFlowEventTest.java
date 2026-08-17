package com.kafkick.core.observation;

import com.kafkick.core.member.Grade;
import org.junit.jupiter.api.Test;

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
    void factoryAndCanonicalConstructorAcceptValidBoundaryValues() {
        IssuanceFlowEvent admitted = FACTORY.admitted(context(null, false), 0L);
        IssuanceFlowEvent issued = FACTORY.issued(context("request-1", false), 1L, "A");

        assertThat(admitted.queueSequence()).isZero();
        assertThat(issued.issuanceId()).isEqualTo(1L);
        assertThat(issued.issuanceCode()).isEqualTo("A");
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

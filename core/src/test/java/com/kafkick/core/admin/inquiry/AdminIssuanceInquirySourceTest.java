package com.kafkick.core.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawHistoryLink;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawIssuance;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;

class AdminIssuanceInquirySourceTest {

    private static final Instant OCCURRED_AT = Instant.parse("2026-08-23T00:00:00Z");

    @Test
    void rejectsNullRowsDuplicateIdsAndLaterListMutation() {
        assertThatThrownBy(() -> new AdminIssuanceInquirySource(null, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquirySource(
                Arrays.asList(attempt(1L), null), List.of(), List.of()))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquirySource(
                List.of(attempt(1L), attempt(1L)), List.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquirySource(
                List.of(), List.of(issuance(1L), issuance(1L)), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceInquirySource(
                List.of(), List.of(), List.of(history(1L), history(1L))))
                .isInstanceOf(IllegalArgumentException.class);

        ArrayList<RawAttempt> attempts = new ArrayList<>(List.of(attempt(1L)));
        AdminIssuanceInquirySource source = new AdminIssuanceInquirySource(
                attempts, List.of(issuance(1L)), List.of(history(1L)));
        attempts.clear();

        assertThat(source.attempts()).hasSize(1);
        assertThatThrownBy(() -> source.attempts().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsNonPositiveIdsAndMissingRequiredValues() {
        assertThatThrownBy(() -> new RawAttempt(
                0L, EventType.ISSUE_ATTEMPT, "request-1", 1L, 2L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_ATTEMPT, "request-1", 0L, 2L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_ATTEMPT, "request-1", 1L, 0L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_ATTEMPT, " ", 1L, 2L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, null, "request-1", 1L, 2L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_ATTEMPT, "request-1", 1L, 2L,
                null, null, null, null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new RawIssuance(
                0L, 1L, 2L, IssuanceStatus.ISSUED, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawIssuance(
                1L, 0L, 2L, IssuanceStatus.ISSUED, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawIssuance(
                1L, 1L, 0L, IssuanceStatus.ISSUED, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawIssuance(
                1L, 1L, 2L, null, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new RawHistoryLink(
                0L, 1L, IssuanceEventType.ISSUE, "request-1", OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawHistoryLink(
                1L, 0L, IssuanceEventType.ISSUE, "request-1", OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawHistoryLink(
                1L, 1L, IssuanceEventType.ISSUE, "", OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsEventsOutsideInquiryPopulationAndInvalidFieldRelationships() {
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_ATTEMPT, "r".repeat(37), 1L, 2L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_RESULT, "request-1", 1L, 2L,
                null, 99, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_RESULT, "request-1", 1L, 2L,
                null, 600, ReasonCode.INTERNAL_ERROR, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ENTRY_RESULT, "request-1", 1L, 2L,
                null, 302, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.QUEUE_ADMITTED, "request-1", 1L, 2L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_ATTEMPT, "request-1", 1L, 2L,
                3L, 201, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ENTRY_RESULT, "request-1", 1L, 2L,
                3L, 200, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_RESULT, "request-1", 1L, 2L,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_RESULT, "request-1", 1L, 2L,
                null, 500, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_RESULT, "request-1", 1L, 2L,
                null, 201, ReasonCode.INTERNAL_ERROR, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawAttempt(
                1L, EventType.ISSUE_RESULT, "request-1", 1L, 2L,
                3L, 409, ReasonCode.ALREADY_ISSUED, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RawHistoryLink(
                1L, 1L, IssuanceEventType.USE, "request-1", OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RawAttempt attempt(long attemptId) {
        return new RawAttempt(
                attemptId, EventType.ISSUE_ATTEMPT, "request-" + attemptId,
                1L, 2L, null, null, null, OCCURRED_AT);
    }

    private static RawIssuance issuance(long issuanceId) {
        return new RawIssuance(
                issuanceId, 1L, 2L, IssuanceStatus.ISSUED, OCCURRED_AT);
    }

    private static RawHistoryLink history(long historyId) {
        return new RawHistoryLink(
                historyId, 1L, IssuanceEventType.ISSUE,
                "request-" + historyId, OCCURRED_AT);
    }
}

package com.kafkick.core.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.InquiryPosition;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryQuery.SourceKind;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquiryResult.InquiryItem;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawHistoryLink;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawIssuance;
import com.kafkick.core.coupon.IssuanceEventType;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;

class IssuanceInquiryCalculatorTest {

    private static final Instant BASE = Instant.parse("2026-08-23T00:00:00Z");
    private final IssuanceInquiryCalculator calculator = new IssuanceInquiryCalculator();

    @Test
    void enrichesAttemptByExactIssuanceIdWithoutDuplicatingLinkedIssuance() {
        RawAttempt attempt = issueResult(
                11L, "request-direct", 101L, 201L, 301L, 201, null, BASE.plusSeconds(10));
        RawIssuance issuance = issuance(
                301L, 101L, 201L, IssuanceStatus.USED, BASE.plusSeconds(8));

        AdminIssuanceInquiryResult result = calculator.calculate(
                source(List.of(attempt), List.of(issuance), List.of()),
                query(101L, null, null, null, null, 50));

        assertThat(result.items()).containsExactly(new InquiryItem(
                101L,
                201L,
                301L,
                201,
                null,
                IssuanceStatus.USED,
                BASE.plusSeconds(10),
                new InquiryPosition(BASE.plusSeconds(10), SourceKind.ATTEMPT, 11L)));
        assertThat(result.hasOlder()).isFalse();
        assertThat(result.nextBefore()).isNull();
    }

    @Test
    void enrichesAttemptByExactIssueHistoryRequestId() {
        RawAttempt attempt = new RawAttempt(
                12L,
                EventType.ENTRY_RESULT,
                "request-history",
                101L,
                202L,
                null,
                200,
                null,
                BASE.plusSeconds(20));
        RawIssuance issuance = issuance(
                302L, 101L, 202L, IssuanceStatus.CANCELLED, BASE.plusSeconds(18));
        RawHistoryLink history = new RawHistoryLink(
                41L,
                302L,
                IssuanceEventType.ISSUE,
                "request-history",
                BASE.plusSeconds(18));

        AdminIssuanceInquiryResult result = calculator.calculate(
                source(List.of(attempt), List.of(issuance), List.of(history)),
                query(101L, null, null, null, null, 50));

        assertThat(result.items()).singleElement().satisfies(item -> {
            assertThat(item.issuanceId()).isEqualTo(302L);
            assertThat(item.currentStatus()).isEqualTo(IssuanceStatus.CANCELLED);
            assertThat(item.position()).isEqualTo(new InquiryPosition(
                    BASE.plusSeconds(20), SourceKind.ATTEMPT, 12L));
        });
    }

    @Test
    void prefersDirectIssuanceIdWhenHistoryRequestPointsToAnotherIssuance() {
        RawAttempt attempt = issueResult(
                13L, "conflicting-links", 101L, 202L, 303L,
                201, null, BASE.plusSeconds(30));
        RawIssuance direct = issuance(
                303L, 101L, 202L, IssuanceStatus.USED, BASE.plusSeconds(10));
        RawIssuance historyTarget = issuance(
                304L, 101L, 202L, IssuanceStatus.CANCELLED, BASE.plusSeconds(20));
        RawHistoryLink history = new RawHistoryLink(
                42L,
                304L,
                IssuanceEventType.ISSUE,
                "conflicting-links",
                BASE.plusSeconds(20));

        AdminIssuanceInquiryResult result = calculator.calculate(
                source(List.of(attempt), List.of(direct, historyTarget), List.of(history)),
                query(101L, null, null, null, null, 50));

        assertThat(result.items()).hasSize(2);
        assertThat(result.items().get(0)).satisfies(item -> {
            assertThat(item.issuanceId()).isEqualTo(303L);
            assertThat(item.currentStatus()).isEqualTo(IssuanceStatus.USED);
            assertThat(item.position().sourceKind()).isEqualTo(SourceKind.ATTEMPT);
        });
        assertThat(result.items().get(1)).satisfies(item -> {
            assertThat(item.issuanceId()).isEqualTo(304L);
            assertThat(item.currentStatus()).isEqualTo(IssuanceStatus.CANCELLED);
            assertThat(item.position().sourceKind()).isEqualTo(SourceKind.ISSUANCE);
        });
    }

    @Test
    void doesNotSynthesizeIssuanceFromDanglingHistoryRequestLink() {
        RawAttempt attempt = new RawAttempt(
                14L,
                EventType.ENTRY_RESULT,
                "dangling-history",
                101L,
                202L,
                null,
                200,
                null,
                BASE.plusSeconds(31));
        RawHistoryLink dangling = new RawHistoryLink(
                43L,
                999L,
                IssuanceEventType.ISSUE,
                "dangling-history",
                BASE.plusSeconds(29));

        AdminIssuanceInquiryResult result = calculator.calculate(
                source(List.of(attempt), List.of(), List.of(dangling)),
                query(101L, null, null, null, null, 50));

        assertThat(result.items()).singleElement().satisfies(item -> {
            assertThat(item.issuanceId()).isNull();
            assertThat(item.currentStatus()).isNull();
            assertThat(item.httpStatus()).isEqualTo(200);
        });
    }

    @Test
    void choosesLatestResultPerRequestAndKeepsSameMemberCouponRetriesSeparate() {
        List<RawAttempt> attempts = List.of(
                issueAttempt(21L, "retry-a", 101L, 203L, BASE.plusSeconds(1)),
                issueFailure(22L, "retry-a", 101L, 203L, 500,
                        ReasonCode.INTERNAL_ERROR, BASE.plusSeconds(2)),
                issueFailure(23L, "retry-a", 101L, 203L, 503,
                        ReasonCode.TEMPORARILY_UNAVAILABLE, BASE.plusSeconds(2)),
                issueFailure(24L, "retry-b", 101L, 203L, 409,
                        ReasonCode.ALREADY_ISSUED, BASE.plusSeconds(3)));

        AdminIssuanceInquiryResult result = calculator.calculate(
                source(attempts, List.of(), List.of()),
                query(101L, null, null, null, null, 50));

        assertThat(result.items()).extracting(InquiryItem::httpStatus)
                .containsExactly(409, 503);
        assertThat(result.items()).extracting(item -> item.position().sourceId())
                .containsExactly(24L, 23L);
    }

    @Test
    void prefersAResultEvenWhenAnAttemptForTheSameRequestWasRecordedLater() {
        RawAttempt result = issueFailure(
                25L, "result-wins", 101L, 203L, 500,
                ReasonCode.INTERNAL_ERROR, BASE.plusSeconds(2));
        RawAttempt laterAttempt = issueAttempt(
                26L, "result-wins", 101L, 203L, BASE.plusSeconds(3));

        AdminIssuanceInquiryResult inquiry = calculator.calculate(
                source(List.of(result, laterAttempt), List.of(), List.of()),
                query(101L, null, null, null, null, 50));

        assertThat(inquiry.items()).singleElement().satisfies(item -> {
            assertThat(item.httpStatus()).isEqualTo(500);
            assertThat(item.position().sourceId()).isEqualTo(25L);
        });
    }

    @Test
    void neverInfersLinksFromMemberCouponAndKeepsAttemptsLinkedToOneIssuanceSeparate() {
        RawAttempt unrelated = issueFailure(
                27L, "unrelated", 101L, 203L, 409,
                ReasonCode.ALREADY_ISSUED, BASE.plusSeconds(7));
        RawIssuance sameMemberCoupon = issuance(
                303L, 101L, 203L, IssuanceStatus.USED, BASE.plusSeconds(6));

        AdminIssuanceInquiryResult unlinked = calculator.calculate(
                source(List.of(unrelated), List.of(sameMemberCoupon), List.of()),
                query(101L, null, null, null, null, 50));

        assertThat(unlinked.items()).hasSize(2);
        assertThat(unlinked.items().get(0).issuanceId()).isNull();
        assertThat(unlinked.items().get(1).issuanceId()).isEqualTo(303L);

        RawAttempt firstRequest = issueResult(
                28L, "linked-a", 101L, 203L, 303L, 201, null, BASE.plusSeconds(9));
        RawAttempt secondRequest = issueResult(
                29L, "linked-b", 101L, 203L, 303L, 201, null, BASE.plusSeconds(8));

        AdminIssuanceInquiryResult linked = calculator.calculate(
                source(List.of(firstRequest, secondRequest), List.of(sameMemberCoupon), List.of()),
                query(101L, null, null, null, null, 50));

        assertThat(linked.items()).hasSize(2);
        assertThat(linked.items()).extracting(InquiryItem::issuanceId)
                .containsExactly(303L, 303L);
        assertThat(linked.items()).extracting(item -> item.position().sourceId())
                .containsExactly(28L, 29L);
    }

    @Test
    void preservesDbOnlyIssuanceAttemptOnlyFailureAndUnconfirmedSuccess() {
        RawAttempt failed = issueFailure(
                31L, "failed-only", 101L, 204L, 500,
                ReasonCode.INTERNAL_ERROR, BASE.plusSeconds(30));
        RawAttempt unconfirmed = issueResult(
                32L, "success-unconfirmed", 101L, 204L, 999L,
                201, null, BASE.plusSeconds(20));
        RawIssuance dbOnly = issuance(
                401L, 101L, 204L, IssuanceStatus.EXPIRED, BASE.plusSeconds(10));

        AdminIssuanceInquiryResult result = calculator.calculate(
                source(List.of(failed, unconfirmed), List.of(dbOnly), List.of()),
                query(101L, null, null, null, null, 50));

        assertThat(result.items()).hasSize(3);
        assertThat(result.items().get(0)).satisfies(item -> {
            assertThat(item.httpStatus()).isEqualTo(500);
            assertThat(item.reasonCode()).isEqualTo(ReasonCode.INTERNAL_ERROR);
            assertThat(item.issuanceId()).isNull();
            assertThat(item.currentStatus()).isNull();
        });
        assertThat(result.items().get(1)).satisfies(item -> {
            assertThat(item.httpStatus()).isEqualTo(201);
            assertThat(item.issuanceId()).isNull();
            assertThat(item.currentStatus()).isNull();
        });
        assertThat(result.items().get(2)).satisfies(item -> {
            assertThat(item.issuanceId()).isEqualTo(401L);
            assertThat(item.currentStatus()).isEqualTo(IssuanceStatus.EXPIRED);
            assertThat(item.httpStatus()).isNull();
            assertThat(item.reasonCode()).isNull();
            assertThat(item.position()).isEqualTo(new InquiryPosition(
                    BASE.plusSeconds(10), SourceKind.ISSUANCE, 401L));
        });
    }

    @Test
    void preservesBareIssueAttemptWithoutInventingResultOrIssuance() {
        RawAttempt attempt = issueAttempt(
                33L, "bare-attempt", 101L, 204L, BASE.plusSeconds(32));

        AdminIssuanceInquiryResult result = calculator.calculate(
                source(List.of(attempt), List.of(), List.of()),
                query(101L, null, null, null, null, 50));

        assertThat(result.items()).singleElement().satisfies(item -> {
            assertThat(item.httpStatus()).isNull();
            assertThat(item.reasonCode()).isNull();
            assertThat(item.issuanceId()).isNull();
            assertThat(item.currentStatus()).isNull();
            assertThat(item.position().sourceId()).isEqualTo(33L);
        });
    }

    @Test
    void appliesMemberCouponHttpStatusAndReasonFiltersAfterJoining() {
        RawAttempt memberOneFailure = issueFailure(
                41L, "member-one", 101L, 205L, 409,
                ReasonCode.ALREADY_ISSUED, BASE.plusSeconds(40));
        RawAttempt memberTwoFailure = issueFailure(
                42L, "member-two", 102L, 205L, 409,
                ReasonCode.ALREADY_ISSUED, BASE.plusSeconds(39));
        RawAttempt otherReason = issueFailure(
                43L, "other-reason", 101L, 206L, 500,
                ReasonCode.INTERNAL_ERROR, BASE.plusSeconds(38));
        RawIssuance dbOnly = issuance(
                402L, 101L, 205L, IssuanceStatus.ISSUED, BASE.plusSeconds(37));
        AdminIssuanceInquirySource source = source(
                List.of(memberOneFailure, memberTwoFailure, otherReason),
                List.of(dbOnly),
                List.of());

        assertThat(calculator.calculate(
                source, query(101L, null, null, null, null, 50)).items())
                .hasSize(3);
        assertThat(calculator.calculate(
                source, query(101L, 205L, null, null, null, 50)).items())
                .hasSize(2);
        assertThat(calculator.calculate(
                source, query(101L, null, 409, null, null, 50)).items())
                .extracting(item -> item.position().sourceId())
                .containsExactly(41L);
        assertThat(calculator.calculate(
                source,
                query(101L, null, null, ReasonCode.ALREADY_ISSUED, null, 50)).items())
                .extracting(item -> item.position().sourceId())
                .containsExactly(41L);
    }

    @Test
    void sortsSameTimeAcrossSourceKindsAndPagesWithLimitPlusOne() {
        Instant tied = BASE.plusSeconds(50);
        RawAttempt lowerAttempt = issueFailure(
                51L, "same-time-a", 101L, 207L, 500,
                ReasonCode.INTERNAL_ERROR, tied);
        RawAttempt higherAttempt = issueFailure(
                52L, "same-time-b", 101L, 207L, 503,
                ReasonCode.TEMPORARILY_UNAVAILABLE, tied);
        RawIssuance sameIdSpace = issuance(
                51L, 101L, 207L, IssuanceStatus.ISSUED, tied);
        AdminIssuanceInquirySource source = source(
                List.of(lowerAttempt, higherAttempt), List.of(sameIdSpace), List.of());

        AdminIssuanceInquiryResult firstPage = calculator.calculate(
                source, query(101L, null, null, null, null, 2));

        assertThat(firstPage.items()).extracting(InquiryItem::position)
                .containsExactly(
                        new InquiryPosition(tied, SourceKind.ISSUANCE, 51L),
                        new InquiryPosition(tied, SourceKind.ATTEMPT, 52L));
        assertThat(firstPage.hasOlder()).isTrue();
        assertThat(firstPage.nextBefore()).isEqualTo(
                new InquiryPosition(tied, SourceKind.ATTEMPT, 52L));

        AdminIssuanceInquiryResult secondPage = calculator.calculate(
                source, query(101L, null, null, null, firstPage.nextBefore(), 2));

        assertThat(secondPage.items()).extracting(InquiryItem::position)
                .containsExactly(new InquiryPosition(tied, SourceKind.ATTEMPT, 51L));
        assertThat(secondPage.hasOlder()).isFalse();
        assertThat(secondPage.nextBefore()).isNull();
    }

    @Test
    void returnsExhaustedMetadataForEmptyExactLimitAndCursorAfterOldest() {
        AdminIssuanceInquiryResult empty = calculator.calculate(
                source(List.of(), List.of(), List.of()),
                query(101L, null, null, null, null, 2));
        assertThat(empty.items()).isEmpty();
        assertThat(empty.hasOlder()).isFalse();
        assertThat(empty.nextBefore()).isNull();

        RawAttempt newer = issueFailure(
                61L, "exact-limit-a", 101L, 208L, 500,
                ReasonCode.INTERNAL_ERROR, BASE.plusSeconds(61));
        RawAttempt older = issueFailure(
                60L, "exact-limit-b", 101L, 208L, 503,
                ReasonCode.TEMPORARILY_UNAVAILABLE, BASE.plusSeconds(60));
        AdminIssuanceInquirySource exactSource = source(
                List.of(newer, older), List.of(), List.of());

        AdminIssuanceInquiryResult exactLimit = calculator.calculate(
                exactSource, query(101L, null, null, null, null, 2));
        assertThat(exactLimit.items()).hasSize(2);
        assertThat(exactLimit.hasOlder()).isFalse();
        assertThat(exactLimit.nextBefore()).isNull();

        InquiryPosition oldest = new InquiryPosition(
                BASE.plusSeconds(60), SourceKind.ATTEMPT, 60L);
        AdminIssuanceInquiryResult exhausted = calculator.calculate(
                exactSource, query(101L, null, null, null, oldest, 2));
        assertThat(exhausted.items()).isEmpty();
        assertThat(exhausted.hasOlder()).isFalse();
        assertThat(exhausted.nextBefore()).isNull();
    }

    private static AdminIssuanceInquirySource source(
            List<RawAttempt> attempts,
            List<RawIssuance> issuances,
            List<RawHistoryLink> histories
    ) {
        return new AdminIssuanceInquirySource(attempts, issuances, histories);
    }

    private static AdminIssuanceInquiryQuery query(
            long memberId,
            Long couponId,
            Integer httpStatus,
            ReasonCode reasonCode,
            InquiryPosition before,
            int limit
    ) {
        return new AdminIssuanceInquiryQuery(
                memberId, couponId, httpStatus, reasonCode, before, limit);
    }

    private static RawAttempt issueResult(
            long attemptId,
            String requestId,
            long memberId,
            long couponId,
            Long issuanceId,
            int httpStatus,
            ReasonCode reasonCode,
            Instant occurredAt
    ) {
        return new RawAttempt(
                attemptId,
                EventType.ISSUE_RESULT,
                requestId,
                memberId,
                couponId,
                issuanceId,
                httpStatus,
                reasonCode,
                occurredAt);
    }

    private static RawAttempt issueFailure(
            long attemptId,
            String requestId,
            long memberId,
            long couponId,
            int httpStatus,
            ReasonCode reasonCode,
            Instant occurredAt
    ) {
        return issueResult(
                attemptId, requestId, memberId, couponId, null,
                httpStatus, reasonCode, occurredAt);
    }

    private static RawAttempt issueAttempt(
            long attemptId,
            String requestId,
            long memberId,
            long couponId,
            Instant occurredAt
    ) {
        return new RawAttempt(
                attemptId,
                EventType.ISSUE_ATTEMPT,
                requestId,
                memberId,
                couponId,
                null,
                null,
                null,
                occurredAt);
    }

    private static RawIssuance issuance(
            long issuanceId,
            long memberId,
            long couponId,
            IssuanceStatus status,
            Instant issuedAt
    ) {
        return new RawIssuance(issuanceId, memberId, couponId, status, issuedAt);
    }
}

package com.kafkick.core.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistoryItem;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.support.exception.BusinessException;

class IssuanceHistoryCalculatorTest {

    private static final Instant T3 = Instant.parse("2026-08-23T03:00:00Z");
    private static final Instant T2 = Instant.parse("2026-08-23T02:00:00Z");
    private static final Instant T1 = Instant.parse("2026-08-23T01:00:00Z");

    private final IssuanceHistoryCalculator calculator = new IssuanceHistoryCalculator(
            new IssuanceCodeMasker());

    @Test
    void filtersTheBusinessPopulationBeforeComputingItsSummary() {
        AdminIssuanceHistoryResult result = calculator.calculate(source(
                issue(1L, 100L, T1),
                use(2L, 100L, T2),
                cancel(3L, 300L, 200L, T2),
                expire(4L, 100L, T3)),
                query(100L, T2, T3, null, null, 50));

        assertThat(result.items()).extracting(item -> item.eventType())
                .containsExactly(IssuanceEventType.USE);
        assertThat(result.summary()).isEqualTo(new HistorySummary(1L, 0L, 1L, 0L, 0L, 0L));
    }

    @Test
    void filtersTheBusinessPopulationByANonNullEventType() {
        AdminIssuanceHistoryResult result = calculator.calculate(source(
                issue(1L, 10L, T3), use(2L, 20L, T2), cancel(3L, 30L, 100L, T1)),
                query(null, null, null, IssuanceEventType.USE, null, 50));

        assertThat(result.items()).extracting(item -> item.eventType())
                .containsExactly(IssuanceEventType.USE);
        assertThat(result.summary()).isEqualTo(new HistorySummary(1L, 0L, 1L, 0L, 0L, 0L));
    }

    @Test
    void summarizesEveryIssuanceEventTypeInTheFilteredPopulation() {
        AdminIssuanceHistoryResult result = calculator.calculate(source(
                issue(1L, 10L, T3),
                use(2L, 20L, T3),
                cancelUse(3L, 30L, T2),
                cancel(4L, 40L, 100L, T2),
                expire(5L, 50L, T1)),
                query(null, null, null, null, null, 50));

        assertThat(result.summary()).isEqualTo(new HistorySummary(5L, 1L, 1L, 1L, 1L, 1L));
    }

    @Test
    void ordersEqualTimestampsByDescendingHistoryIdAndAppliesCursorBoundary() {
        AdminIssuanceHistoryResult result = calculator.calculate(source(
                issue(1L, 1L, T2),
                issue(3L, 3L, T2),
                issue(2L, 2L, T2)),
                query(null, null, null, null, new HistoryPosition(T2, 3L), 50));

        assertThat(result.items()).extracting(item -> item.issuanceId())
                .containsExactly(2L, 1L);
    }

    @Test
    void usesOneExtraCandidateToIndicateOlderHistoryAndBuildsNextCursor() {
        AdminIssuanceHistoryResult result = calculator.calculate(source(
                issue(1L, 1L, T3), issue(2L, 2L, T2), issue(3L, 3L, T1)),
                query(null, null, null, null, null, 2));

        assertThat(result.items()).extracting(item -> item.issuanceId()).containsExactly(1L, 2L);
        assertThat(result.hasOlder()).isTrue();
        assertThat(result.nextBefore()).isEqualTo(new HistoryPosition(T2, 2L));
        assertThat(result.summary()).isEqualTo(new HistorySummary(3L, 3L, 0L, 0L, 0L, 0L));
    }

    @Test
    void doesNotExposeACursorWhenThePageHasNoOlderHistory() {
        AdminIssuanceHistoryResult result = calculator.calculate(source(issue(1L, 100L, T1)),
                query(null, null, null, null, null, 1));

        assertThat(result.hasOlder()).isFalse();
        assertThat(result.nextBefore()).isNull();
        assertThat(result.items().getFirst().issuanceCodeMasked()).isEqualTo("ABCD********5678");
    }

    @Test
    void rejectsInvalidQueryAndResultInvariants() {
        assertThatThrownBy(() -> query(0L, null, null, null, null, 1))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> query(null, T2, T2, null, null, 1))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> query(null, T3, T2, null, null, 1))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> query(null, null, null, null, null, 0))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> query(null, null, null, null, null, 201))
                .isInstanceOf(BusinessException.class);
        assertThatThrownBy(() -> new HistoryPosition(T1, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistoryResult(
                Arrays.asList((HistoryItem) null), null, false,
                new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistoryResult(
                List.of(), new HistoryPosition(T1, 1L), false,
                new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistoryResult(
                List.of(), new HistoryPosition(T1, 1L), true,
                new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistoryResult(
                List.of(historyItem()), null, true,
                new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistoryResult(
                null, null, false, new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new HistorySummary(2L, 1L, 1L, 1L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(-1L, 0L, 0L, 0L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(0L, -1L, 0L, 0L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(0L, 0L, -1L, 0L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(0L, 0L, 0L, -1L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(0L, 0L, 0L, 0L, -1L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(0L, 0L, 0L, 0L, 0L, -1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(0L, -1L, 1L, 0L, 0L, 0L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HistorySummary(
                Long.MAX_VALUE, Long.MAX_VALUE, 1L, 0L, 0L, 0L))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void protectsResultItemsFromLaterListChanges() {
        ArrayList<HistoryItem> items = new ArrayList<>();
        items.add(historyItem());

        AdminIssuanceHistoryResult result = new AdminIssuanceHistoryResult(
                items, null, false, new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L));
        items.clear();

        assertThat(result.items()).containsExactly(historyItem());
        assertThatThrownBy(() -> result.items().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static AdminIssuanceHistoryQuery query(
            Long couponId,
            Instant fromInclusive,
            Instant toExclusive,
            IssuanceEventType eventType,
            HistoryPosition before,
            int limit
    ) {
        return new AdminIssuanceHistoryQuery(
                couponId, fromInclusive, toExclusive, eventType, before, limit);
    }

    private static AdminIssuanceHistorySource source(AdminIssuanceHistorySource.RawHistory... rows) {
        return new AdminIssuanceHistorySource(List.of(rows));
    }

    private static AdminIssuanceHistorySource.RawHistory issue(
            long historyId,
            long issuanceId,
            Instant occurredAt
    ) {
        return history(historyId, issuanceId, 100L, IssuanceStatus.ISSUED, IssuanceEventType.ISSUE,
                occurredAt);
    }

    private static AdminIssuanceHistorySource.RawHistory use(
            long historyId,
            long issuanceId,
            Instant occurredAt
    ) {
        return history(historyId, issuanceId, 100L, IssuanceStatus.USED, IssuanceEventType.USE,
                occurredAt);
    }

    private static AdminIssuanceHistorySource.RawHistory cancel(
            long historyId,
            long issuanceId,
            long couponId,
            Instant occurredAt
    ) {
        return history(historyId, issuanceId, couponId, IssuanceStatus.CANCELLED,
                IssuanceEventType.CANCEL, occurredAt);
    }

    private static AdminIssuanceHistorySource.RawHistory cancelUse(
            long historyId,
            long issuanceId,
            Instant occurredAt
    ) {
        return history(historyId, issuanceId, 100L, IssuanceStatus.ISSUED,
                IssuanceEventType.CANCEL_USE, occurredAt);
    }

    private static AdminIssuanceHistorySource.RawHistory expire(
            long historyId,
            long issuanceId,
            Instant occurredAt
    ) {
        return history(historyId, issuanceId, 100L, IssuanceStatus.EXPIRED, IssuanceEventType.EXPIRE,
                occurredAt);
    }

    private static AdminIssuanceHistorySource.RawHistory history(
            long historyId,
            long issuanceId,
            long couponId,
            IssuanceStatus toStatus,
            IssuanceEventType eventType,
            Instant occurredAt
    ) {
        IssuanceStatus fromStatus = switch (eventType) {
            case ISSUE -> null;
            case CANCEL_USE -> IssuanceStatus.USED;
            case USE, CANCEL, EXPIRE -> IssuanceStatus.ISSUED;
        };
        return new AdminIssuanceHistorySource.RawHistory(
                historyId,
                issuanceId,
                "ABCD1234EFGH5678",
                couponId,
                fromStatus,
                toStatus,
                eventType,
                null,
                null,
                occurredAt);
    }

    private static HistoryItem historyItem() {
        return new HistoryItem(
                10L,
                "ABCD********5678",
                100L,
                null,
                IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE,
                T1);
    }
}

package com.kafkick.core.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

class AdminIssuanceHistorySourceTest {

    private static final Instant OCCURRED_AT = Instant.parse("2026-08-23T00:00:00Z");

    @Test
    void rejectsNullAndDuplicateHistoryRows() {
        assertThatThrownBy(() -> new AdminIssuanceHistorySource(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource(Arrays.asList(row(1L), null)))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource(List.of(row(1L), row(1L))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void protectsHistoryRowsFromLaterListChanges() {
        ArrayList<AdminIssuanceHistorySource.RawHistory> rows = new ArrayList<>();
        rows.add(row(1L));

        AdminIssuanceHistorySource source = new AdminIssuanceHistorySource(rows);
        rows.clear();

        assertThat(source.histories()).hasSize(1);
        assertThatThrownBy(() -> source.histories().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsInvalidRawHistoryInvariants() {
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                0L, 10L, "ABCD1234EFGH5678", 20L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 0L, "ABCD1234EFGH5678", 20L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "ABCD1234EFGH5678", 0L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "short", 20L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "ABCD1234EFGH5678", 20L, null, null,
                IssuanceEventType.ISSUE, null, null, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "ABCD1234EFGH5678", 20L, null, IssuanceStatus.ISSUED,
                null, null, null, OCCURRED_AT))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "ABCD1234EFGH5678", 20L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, null, null, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "ABCD1234EFGH5678", 20L, IssuanceStatus.USED, IssuanceStatus.ISSUED,
                IssuanceEventType.USE, null, null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "ABCD1234EFGH5678", 20L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, "", null, OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AdminIssuanceHistorySource.RawHistory(
                1L, 10L, "ABCD1234EFGH5678", 20L, null, IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE, null, "", OCCURRED_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static AdminIssuanceHistorySource.RawHistory row(long historyId) {
        return new AdminIssuanceHistorySource.RawHistory(
                historyId,
                10L,
                "ABCD1234EFGH5678",
                20L,
                null,
                IssuanceStatus.ISSUED,
                IssuanceEventType.ISSUE,
                null,
                null,
                OCCURRED_AT);
    }
}

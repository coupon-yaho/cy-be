package com.kafkick.core.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryQuery.HistoryPosition;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.coupon.domain.IssuanceEventType;
import com.kafkick.core.coupon.domain.IssuanceStatus;

class IssuanceHistoryCalculatorTest {
    @Test
    void createsCursorFromTheLastReturnedRowAndKeepsReaderSummary() {
        Instant newest = Instant.parse("2026-08-23T03:00:00Z");
        Instant older = Instant.parse("2026-08-23T02:00:00Z");
        HistorySummary summary = new HistorySummary(3L, 3L, 0L, 0L, 0L, 0L);
        AdminIssuanceHistoryReadResult readResult = new AdminIssuanceHistoryReadResult(
                List.of(row(3L, 3L, newest), row(2L, 2L, older), row(1L, 1L, older)), summary);

        AdminIssuanceHistoryResult result = new IssuanceHistoryCalculator(new IssuanceCodeMasker())
                .calculate(readResult, 2);

        assertThat(result.items()).extracting(item -> item.issuanceId()).containsExactly(3L, 2L);
        assertThat(result.nextBefore()).isEqualTo(new HistoryPosition(older, 2L));
        assertThat(result.hasOlder()).isTrue();
        assertThat(result.summary()).isSameAs(summary);
        assertThat(result.items().getFirst().issuanceCodeMasked()).isEqualTo("ABCD********5678");
    }

    @Test
    void omitsCursorWhenReaderReturnsAtMostTheLimit() {
        AdminIssuanceHistoryResult result = new IssuanceHistoryCalculator(new IssuanceCodeMasker())
                .calculate(new AdminIssuanceHistoryReadResult(
                        List.of(row(1L, 1L, Instant.parse("2026-08-23T01:00:00Z"))),
                        new HistorySummary(1L, 1L, 0L, 0L, 0L, 0L)), 1);

        assertThat(result.hasOlder()).isFalse();
        assertThat(result.nextBefore()).isNull();
    }

    private static AdminIssuanceHistorySource.RawHistory row(long historyId, long issuanceId, Instant at) {
        return new AdminIssuanceHistorySource.RawHistory(historyId, issuanceId, "ABCD1234EFGH5678",
                100L, null, IssuanceStatus.ISSUED, IssuanceEventType.ISSUE, null, null, at);
    }
}

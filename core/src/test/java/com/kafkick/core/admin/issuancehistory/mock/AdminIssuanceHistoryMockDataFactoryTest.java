package com.kafkick.core.admin.issuancehistory.mock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource;
import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistorySource.RawHistory;
import com.kafkick.core.coupon.IssuanceEventType;

/** Verifies the deterministic raw issuance-history rows supplied to the Core calculator. */
class AdminIssuanceHistoryMockDataFactoryTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");
    private static final Instant NON_MIDNIGHT_SNAPSHOT = Instant.parse("2026-08-22T12:34:00Z");
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private final AdminIssuanceHistoryMockDataFactory factory =
            new AdminIssuanceHistoryMockDataFactory();

    /** Detects a Factory that derives data from an unstable or repeated clock read. */
    @Test
    void createsTheSameRawSourceForTheSameSnapshot() {
        AdminIssuanceHistorySource first = factory.create(SNAPSHOT_AT);
        AdminIssuanceHistorySource second = factory.create(SNAPSHOT_AT);

        assertThat(first).isEqualTo(second);
        assertThat(first.histories()).allSatisfy(history ->
                assertThat(history.occurredAt()).isBeforeOrEqualTo(SNAPSHOT_AT));
    }

    /** Detects a Dataset that cannot exercise every status-change or code-masking scenario. */
    @Test
    void providesBothCouponsEveryEventTypeAndDistinctMaskingCodes() {
        List<RawHistory> histories = factory.create(SNAPSHOT_AT).histories();

        assertThat(histories).extracting(RawHistory::couponId).contains(101L, 102L);
        assertThat(histories).extracting(RawHistory::eventType)
                .containsExactlyInAnyOrder(
                        IssuanceEventType.ISSUE,
                        IssuanceEventType.ISSUE,
                        IssuanceEventType.USE,
                        IssuanceEventType.CANCEL_USE,
                        IssuanceEventType.CANCEL,
                        IssuanceEventType.EXPIRE,
                        IssuanceEventType.ISSUE,
                        IssuanceEventType.CANCEL);
        assertThat(histories).extracting(RawHistory::issuanceCode)
                .contains("A101000000000001", "B102000000000002");
    }

    /** Detects removal of the KST date boundaries or the duplicate-timestamp cursor scenario. */
    @Test
    void providesKstDayBoundariesAndEqualTimestampRowsForFilterAndCursorCoverage() {
        List<RawHistory> histories = factory.create(NON_MIDNIGHT_SNAPSHOT).histories();
        LocalDate kstDate = NON_MIDNIGHT_SNAPSHOT.atZone(KST).toLocalDate();
        Instant previousKstDayStart = kstDate.minusDays(1).atStartOfDay(KST).toInstant();
        Instant currentKstDayStart = kstDate.atStartOfDay(KST).toInstant();
        Instant equalTimestamp = NON_MIDNIGHT_SNAPSHOT.minus(Duration.ofHours(1));

        assertThat(histories).extracting(RawHistory::occurredAt)
                .contains(previousKstDayStart, currentKstDayStart);
        assertThat(histories).allSatisfy(history ->
                assertThat(history.occurredAt()).isBeforeOrEqualTo(NON_MIDNIGHT_SNAPSHOT));
        assertThat(histories.stream()
                .filter(history -> history.occurredAt().equals(equalTimestamp))
                .map(RawHistory::historyId)
                .toList())
                .containsExactlyInAnyOrder(1_007L, 1_008L);
    }
}

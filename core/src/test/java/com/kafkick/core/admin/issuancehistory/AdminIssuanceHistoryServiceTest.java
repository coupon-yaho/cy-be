package com.kafkick.core.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.support.TimeProvider;

/** Verifies the Core service assembles one request from one time and one reader result. */
class AdminIssuanceHistoryServiceTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");

    /** Detects duplicate dependency calls or a service that recreates the calculator result. */
    @Test
    void readsTimeReadsOnceAndCalculatesExactlyOnceThenReturnsTheCalculatorResult() {
        RecordingTimeProvider timeProvider = new RecordingTimeProvider();
        RecordingReader reader = new RecordingReader();
        AdminIssuanceHistoryResult expected = new AdminIssuanceHistoryResult(
                List.of(), null, false, new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L));
        RecordingCalculator calculator = new RecordingCalculator(expected);
        AdminIssuanceHistoryService service = new AdminIssuanceHistoryService(
                timeProvider, reader, calculator);
        AdminIssuanceHistoryQuery query = new AdminIssuanceHistoryQuery(
                101L, null, null, null, null, AdminIssuanceHistoryQuery.DEFAULT_LIMIT);

        AdminIssuanceHistoryResult result = service.getHistories(query);

        assertThat(result).isSameAs(expected);
        assertThat(timeProvider.instantCount).isEqualTo(1);
        assertThat(reader.readCount).isEqualTo(1);
        assertThat(reader.lastQuery).isSameAs(query);
        assertThat(reader.lastSnapshotAt).isEqualTo(SNAPSHOT_AT);
        assertThat(calculator.calculateCount).isEqualTo(1);
        assertThat(calculator.lastReadResult).isSameAs(reader.readResult);
        assertThat(calculator.lastLimit).isEqualTo(query.limit());
    }

    /** Counts request-time reads while preserving a fixed, externally observable instant. */
    private static final class RecordingTimeProvider extends TimeProvider {

        private int instantCount;

        /** Creates the recording provider at the test request's fixed instant. */
        private RecordingTimeProvider() {
            super(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC));
        }

        /** Records each request-time read before returning the fixed instant. */
        @Override
        public Instant instant() {
            instantCount++;
            return super.instant();
        }
    }

    /** Records Reader calls and supplies an otherwise valid empty read result. */
    private static final class RecordingReader implements AdminIssuanceHistoryReader {

        private int readCount;
        private AdminIssuanceHistoryQuery lastQuery;
        private Instant lastSnapshotAt;
        private AdminIssuanceHistoryReadResult readResult;

        /** Records the Reader boundary without calculating or enriching the raw source. */
        @Override
        public AdminIssuanceHistoryReadResult read(
                AdminIssuanceHistoryQuery query,
                Instant snapshotAt
        ) {
            readCount++;
            lastQuery = query;
            lastSnapshotAt = snapshotAt;
            readResult = new AdminIssuanceHistoryReadResult(
                    List.of(), new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L));
            return readResult;
        }
    }

    /** Records calculator inputs and exposes an identity-preserving expected result. */
    private static final class RecordingCalculator extends IssuanceHistoryCalculator {

        private final AdminIssuanceHistoryResult expected;
        private int calculateCount;
        private AdminIssuanceHistoryReadResult lastReadResult;
        private int lastLimit;

        /** Creates a calculator double that returns the specified prebuilt result instance. */
        private RecordingCalculator(AdminIssuanceHistoryResult expected) {
            super(new IssuanceCodeMasker());
            this.expected = expected;
        }

        /** Records calculator invocation arguments before returning the prepared result unchanged. */
        @Override
        public AdminIssuanceHistoryResult calculate(
                AdminIssuanceHistoryReadResult readResult,
                int limit
        ) {
            calculateCount++;
            lastReadResult = readResult;
            lastLimit = limit;
            return expected;
        }
    }
}

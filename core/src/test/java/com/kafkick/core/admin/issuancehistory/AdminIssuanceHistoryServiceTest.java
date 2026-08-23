package com.kafkick.core.admin.issuancehistory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.issuancehistory.AdminIssuanceHistoryResult.HistorySummary;
import com.kafkick.core.admin.issuancehistory.mock.AdminIssuanceHistoryMockDataFactory;
import com.kafkick.core.support.TimeProvider;

/** Verifies the Core service assembles one request from one time and one raw source. */
class AdminIssuanceHistoryServiceTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");

    /** Detects duplicate dependency calls or a service that recreates the calculator result. */
    @Test
    void readsTimeCreatesSourceAndCalculatesExactlyOnceThenReturnsTheCalculatorResult() {
        RecordingTimeProvider timeProvider = new RecordingTimeProvider();
        RecordingMockDataFactory factory = new RecordingMockDataFactory();
        AdminIssuanceHistoryResult expected = new AdminIssuanceHistoryResult(
                List.of(), null, false, new HistorySummary(0L, 0L, 0L, 0L, 0L, 0L));
        RecordingCalculator calculator = new RecordingCalculator(expected);
        AdminIssuanceHistoryService service = new AdminIssuanceHistoryService(
                timeProvider, factory, calculator);
        AdminIssuanceHistoryQuery query = new AdminIssuanceHistoryQuery(
                101L, null, null, null, null, AdminIssuanceHistoryQuery.DEFAULT_LIMIT);

        AdminIssuanceHistoryResult result = service.getHistories(query);

        assertThat(result).isSameAs(expected);
        assertThat(timeProvider.instantCount).isEqualTo(1);
        assertThat(factory.createCount).isEqualTo(1);
        assertThat(factory.lastSnapshotAt).isEqualTo(SNAPSHOT_AT);
        assertThat(calculator.calculateCount).isEqualTo(1);
        assertThat(calculator.lastSource).isSameAs(factory.source);
        assertThat(calculator.lastQuery).isSameAs(query);
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

    /** Records Factory calls and supplies an otherwise valid empty raw source. */
    private static final class RecordingMockDataFactory extends AdminIssuanceHistoryMockDataFactory {

        private int createCount;
        private Instant lastSnapshotAt;
        private AdminIssuanceHistorySource source;

        /** Records the Factory boundary without calculating or enriching the raw source. */
        @Override
        public AdminIssuanceHistorySource create(Instant snapshotAt) {
            createCount++;
            lastSnapshotAt = snapshotAt;
            source = new AdminIssuanceHistorySource(List.of());
            return source;
        }
    }

    /** Records calculator inputs and exposes an identity-preserving expected result. */
    private static final class RecordingCalculator extends IssuanceHistoryCalculator {

        private final AdminIssuanceHistoryResult expected;
        private int calculateCount;
        private AdminIssuanceHistorySource lastSource;
        private AdminIssuanceHistoryQuery lastQuery;

        /** Creates a calculator double that returns the specified prebuilt result instance. */
        private RecordingCalculator(AdminIssuanceHistoryResult expected) {
            super(new IssuanceCodeMasker());
            this.expected = expected;
        }

        /** Records calculator invocation arguments before returning the prepared result unchanged. */
        @Override
        public AdminIssuanceHistoryResult calculate(
                AdminIssuanceHistorySource source,
                AdminIssuanceHistoryQuery query
        ) {
            calculateCount++;
            lastSource = source;
            lastQuery = query;
            return expected;
        }
    }
}

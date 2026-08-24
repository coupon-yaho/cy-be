package com.kafkick.core.admin.inquiry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.mock.AdminIssuanceInquiryMockDataFactory;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

class AdminIssuanceInquiryServiceTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-23T00:00:00Z");

    @Test
    void readsQueryAndSnapshotThenCalculatesExactlyOnce() {
        RecordingTimeProvider timeProvider = new RecordingTimeProvider();
        RecordingReader reader = new RecordingReader();
        AdminIssuanceInquiryResult expected = new AdminIssuanceInquiryResult(
                List.of(), null, false);
        RecordingCalculator calculator = new RecordingCalculator(expected);
        AdminIssuanceInquiryService service = new AdminIssuanceInquiryService(
                timeProvider, reader, calculator);
        AdminIssuanceInquiryQuery query = query(
                1_001L, null, null, null, null, 50);

        AdminIssuanceInquiryResult result = service.getInquiries(query);

        assertThat(result).isSameAs(expected);
        assertThat(timeProvider.instantCount).isEqualTo(1);
        assertThat(reader.readCount).isEqualTo(1);
        assertThat(reader.lastQuery).isSameAs(query);
        assertThat(reader.lastSnapshotAt).isEqualTo(SNAPSHOT_AT);
        assertThat(calculator.calculateCount).isEqualTo(1);
        assertThat(calculator.lastSource).isSameAs(reader.source);
        assertThat(calculator.lastQuery).isSameAs(query);
    }

    @Test
    void turnsMissingMemberIntoMemberSpecificNotFound() {
        RecordingReader reader = new RecordingReader(
                AdminIssuanceInquiryReadResult.memberNotFound());
        RecordingCalculator calculator = new RecordingCalculator(
                new AdminIssuanceInquiryResult(List.of(), null, false));
        AdminIssuanceInquiryService service = new AdminIssuanceInquiryService(
                new TimeProvider(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC)), reader, calculator);

        assertThatThrownBy(() -> service.getInquiries(query(
                1_001L, null, null, null, null, 50)))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode())
                                .isEqualTo(AdminIssuanceInquiryErrorCode.MEMBER_NOT_FOUND));
        assertThat(calculator.calculateCount).isZero();
    }

    @Test
    void turnsMissingCouponIntoCouponSpecificNotFound() {
        RecordingReader reader = new RecordingReader(
                AdminIssuanceInquiryReadResult.couponNotFound());
        RecordingCalculator calculator = new RecordingCalculator(
                new AdminIssuanceInquiryResult(List.of(), null, false));
        AdminIssuanceInquiryService service = new AdminIssuanceInquiryService(
                new TimeProvider(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC)), reader, calculator);

        assertThatThrownBy(() -> service.getInquiries(query(
                1_001L, 2_001L, null, null, null, 50)))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode())
                                .isEqualTo(AdminIssuanceInquiryErrorCode.COUPON_NOT_FOUND));
        assertThat(calculator.calculateCount).isZero();
    }

    @Test
    void preservesMemberNotFoundPrecedenceWhenMemberAndCouponAreBothMissing() {
        RecordingReader reader = new RecordingReader(
                AdminIssuanceInquiryReadResult.memberNotFound());
        AdminIssuanceInquiryService service = new AdminIssuanceInquiryService(
                new TimeProvider(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC)), reader,
                new RecordingCalculator(new AdminIssuanceInquiryResult(List.of(), null, false)));

        assertThatThrownBy(() -> service.getInquiries(query(
                1_001L, 2_001L, null, null, null, 50)))
                .isInstanceOfSatisfying(BusinessException.class, exception ->
                        assertThat(exception.getErrorCode())
                                .isEqualTo(AdminIssuanceInquiryErrorCode.MEMBER_NOT_FOUND));
    }

    @Test
    void returnsAnEmptyPageWhenAvailableSourceHasNoCandidates() {
        AdminIssuanceInquiryService service = new AdminIssuanceInquiryService(
                new TimeProvider(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC)),
                (query, snapshotAt) -> AdminIssuanceInquiryReadResult.available(
                        new AdminIssuanceInquirySource(List.of(), List.of(), List.of())),
                new IssuanceInquiryCalculator());

        AdminIssuanceInquiryResult result = service.getInquiries(query(
                1_001L, null, null, null, null, 50));

        assertThat(result.items()).isEmpty();
        assertThat(result.hasOlder()).isFalse();
        assertThat(result.nextBefore()).isNull();
    }

    @Test
    void returnsMemberResultsIncludingDbOnlyAndSeparateRetries() {
        AdminIssuanceInquiryResult result = service().getInquiries(query(
                1_001L, null, null, null, null, 50));

        assertThat(result.items()).hasSize(10);
        assertThat(result.items()).filteredOn(item -> item.couponId() == 2_005L)
                .singleElement()
                .satisfies(item -> {
                    assertThat(item.issuanceId()).isEqualTo(5_003L);
                    assertThat(item.httpStatus()).isNull();
                    assertThat(item.reasonCode()).isNull();
                });
        assertThat(result.items()).filteredOn(item -> item.couponId() == 2_004L)
                .extracting(item -> item.position().sourceId())
                .containsExactly(108L, 107L);
    }

    @Test
    void appliesPolicyReasonAndUnconfirmedSuccessSemantics() {
        AdminIssuanceInquiryService service = service();

        AdminIssuanceInquiryResult policy = service.getInquiries(query(
                1_001L, null, null, ReasonCode.ALREADY_ISSUED, null, 50));
        assertThat(policy.items()).extracting(item -> item.position().sourceId())
                .containsExactly(107L, 104L);
        assertThat(policy.items()).allSatisfy(item -> {
            assertThat(item.httpStatus()).isEqualTo(409);
            assertThat(item.reasonCode()).isEqualTo(ReasonCode.ALREADY_ISSUED);
            assertThat(item.issuanceId()).isNull();
            assertThat(item.currentStatus()).isNull();
        });

        AdminIssuanceInquiryResult systemFailures = service.getInquiries(query(
                1_001L, null, 500, ReasonCode.INTERNAL_ERROR, null, 50));
        assertThat(systemFailures.items()).filteredOn(item -> item.position().sourceId() == 105L)
                .singleElement()
                .satisfies(item -> {
                    assertThat(item.httpStatus()).isEqualTo(500);
                    assertThat(item.reasonCode()).isEqualTo(ReasonCode.INTERNAL_ERROR);
                    assertThat(item.issuanceId()).isNull();
                    assertThat(item.currentStatus()).isNull();
                });

        AdminIssuanceInquiryResult success = service.getInquiries(query(
                1_001L, null, 201, null, null, 50));
        assertThat(success.items()).hasSize(2);
        assertThat(success.items()).filteredOn(item -> item.couponId() == 2_003L)
                .singleElement()
                .satisfies(item -> {
                    assertThat(item.issuanceId()).isNull();
                    assertThat(item.currentStatus()).isNull();
                });
    }

    @Test
    void pagesTiedAttemptRowsWithoutDuplication() {
        AdminIssuanceInquiryService firstService = service(SNAPSHOT_AT);
        AdminIssuanceInquiryResult first = firstService.getInquiries(query(
                1_001L, 2_006L, null, null, null, 1));
        AdminIssuanceInquiryService laterService = service(SNAPSHOT_AT.plusSeconds(3_600));
        AdminIssuanceInquiryResult second = laterService.getInquiries(query(
                1_001L, 2_006L, null, null, first.nextBefore(), 1));

        assertThat(first.items()).extracting(item -> item.position().sourceId())
                .containsExactly(110L);
        assertThat(first.hasOlder()).isTrue();
        assertThat(second.items()).extracting(item -> item.position().sourceId())
                .containsExactly(109L);
        assertThat(second.hasOlder()).isFalse();
    }

    private static AdminIssuanceInquiryService service() {
        return service(SNAPSHOT_AT);
    }

    private static AdminIssuanceInquiryService service(Instant snapshotAt) {
        return new AdminIssuanceInquiryService(
                new TimeProvider(Clock.fixed(snapshotAt, ZoneOffset.UTC)),
                new AdminIssuanceInquiryMockDataFactory(),
                new IssuanceInquiryCalculator());
    }

    private static AdminIssuanceInquiryQuery query(
            long memberId,
            Long couponId,
            Integer httpStatus,
            ReasonCode reasonCode,
            AdminIssuanceInquiryQuery.InquiryPosition before,
            int limit
    ) {
        return new AdminIssuanceInquiryQuery(
                memberId, couponId, httpStatus, reasonCode, before, limit);
    }

    private static final class RecordingTimeProvider extends TimeProvider {

        private int instantCount;

        private RecordingTimeProvider() {
            super(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC));
        }

        @Override
        public Instant instant() {
            instantCount++;
            return super.instant();
        }
    }

    private static final class RecordingReader implements AdminIssuanceInquirySourceReader {

        private final AdminIssuanceInquiryReadResult readResult;
        private int readCount;
        private AdminIssuanceInquiryQuery lastQuery;
        private Instant lastSnapshotAt;
        private AdminIssuanceInquirySource source;

        private RecordingReader() {
            this(null);
        }

        private RecordingReader(AdminIssuanceInquiryReadResult readResult) {
            this.readResult = readResult;
        }

        @Override
        public AdminIssuanceInquiryReadResult read(
                AdminIssuanceInquiryQuery query,
                Instant snapshotAt
        ) {
            readCount++;
            lastQuery = query;
            lastSnapshotAt = snapshotAt;
            source = new AdminIssuanceInquirySource(
                    List.of(new RawAttempt(
                            1L,
                            EventType.ISSUE_ATTEMPT,
                            "recording-request",
                            1_001L,
                            2_001L,
                            null,
                            null,
                            null,
                            SNAPSHOT_AT)),
                    List.of(),
                    List.of());
            return readResult == null
                    ? AdminIssuanceInquiryReadResult.available(source)
                    : readResult;
        }
    }

    private static final class RecordingCalculator extends IssuanceInquiryCalculator {

        private final AdminIssuanceInquiryResult expected;
        private int calculateCount;
        private AdminIssuanceInquirySource lastSource;
        private AdminIssuanceInquiryQuery lastQuery;

        private RecordingCalculator(AdminIssuanceInquiryResult expected) {
            this.expected = expected;
        }

        @Override
        public AdminIssuanceInquiryResult calculate(
                AdminIssuanceInquirySource source,
                AdminIssuanceInquiryQuery query
        ) {
            calculateCount++;
            lastSource = source;
            lastQuery = query;
            return expected;
        }
    }
}

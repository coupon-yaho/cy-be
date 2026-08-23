package com.kafkick.core.admin.inquiry.mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawAttempt;
import com.kafkick.core.admin.inquiry.AdminIssuanceInquirySource.RawIssuance;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.observation.EventType;
import com.kafkick.core.observation.ReasonCode;

class AdminIssuanceInquiryMockDataFactoryTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-23T00:00:00Z");
    private final AdminIssuanceInquiryMockDataFactory factory =
            new AdminIssuanceInquiryMockDataFactory();

    @Test
    void createsStableRawDbRowsAcrossFactoriesAndLaterRequestTimes() {
        AdminIssuanceInquirySource first = factory.create(SNAPSHOT_AT);
        AdminIssuanceInquirySource second = new AdminIssuanceInquiryMockDataFactory()
                .create(SNAPSHOT_AT.plus(Duration.ofHours(1)));

        assertThat(first).isEqualTo(second);
        assertThat(first.attempts()).hasSize(10);
        assertThat(first.issuances()).hasSize(3);
        assertThat(first.histories()).hasSize(1);
        assertThat(first.attempts()).allSatisfy(row ->
                assertThat(row.occurredAt()).isBeforeOrEqualTo(SNAPSHOT_AT));
        assertThat(first.issuances()).allSatisfy(row ->
                assertThat(row.issuedAt()).isBeforeOrEqualTo(SNAPSHOT_AT));
    }

    @Test
    void enforcesSnapshotBoundaryAtNewestFixedRow() {
        Instant newestRow = SNAPSHOT_AT.minus(Duration.ofMinutes(2));

        assertThat(factory.create(newestRow).attempts()).hasSize(10);
        assertThatThrownBy(() -> factory.create(newestRow.minusNanos(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> factory.create(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void providesDirectAndHistoryRequestLinkScenarios() {
        AdminIssuanceInquirySource source = factory.create(SNAPSHOT_AT);

        RawAttempt direct = attempt(source, 102L);
        assertThat(direct.requestId()).isEqualTo("inquiry-direct");
        assertThat(direct.issuanceId()).isEqualTo(5_001L);
        assertThat(source.issuances()).extracting(RawIssuance::issuanceId)
                .contains(5_001L);

        RawAttempt historyLinked = attempt(source, 103L);
        assertThat(historyLinked.requestId()).isEqualTo("inquiry-history");
        assertThat(historyLinked.issuanceId()).isNull();
        assertThat(source.histories()).singleElement().satisfies(history -> {
            assertThat(history.requestId()).isEqualTo("inquiry-history");
            assertThat(history.issuanceId()).isEqualTo(5_002L);
        });
        assertThat(source.issuances()).filteredOn(row -> row.issuanceId() == 5_002L)
                .singleElement()
                .extracting(RawIssuance::currentStatus)
                .isEqualTo(IssuanceStatus.CANCELLED);
    }

    @Test
    void providesPolicySystemDbOnlyAndUnconfirmedSuccessScenarios() {
        AdminIssuanceInquirySource source = factory.create(SNAPSHOT_AT);

        assertThat(attempt(source, 104L)).satisfies(row -> {
            assertThat(row.httpStatus()).isEqualTo(409);
            assertThat(row.reasonCode()).isEqualTo(ReasonCode.ALREADY_ISSUED);
            assertThat(row.issuanceId()).isNull();
        });
        assertThat(attempt(source, 105L)).satisfies(row -> {
            assertThat(row.httpStatus()).isEqualTo(500);
            assertThat(row.reasonCode()).isEqualTo(ReasonCode.INTERNAL_ERROR);
            assertThat(row.issuanceId()).isNull();
        });
        assertThat(source.issuances()).filteredOn(row -> row.issuanceId() == 5_003L)
                .singleElement()
                .satisfies(row -> assertThat(source.attempts())
                        .noneMatch(attempt -> Objects.equals(
                                attempt.issuanceId(), row.issuanceId())));
        assertThat(attempt(source, 106L)).satisfies(row -> {
            assertThat(row.httpStatus()).isEqualTo(201);
            assertThat(row.issuanceId()).isEqualTo(5_999L);
            assertThat(source.issuances()).noneMatch(
                    issuance -> issuance.issuanceId() == row.issuanceId());
        });
    }

    @Test
    void providesSeparateRetriesTiedRowsAndAttemptResultPairButNoQueueAdmission() {
        AdminIssuanceInquirySource source = factory.create(SNAPSHOT_AT);

        assertThat(source.attempts()).filteredOn(row -> row.couponId() == 2_004L)
                .extracting(RawAttempt::requestId)
                .containsExactlyInAnyOrder("inquiry-retry-a", "inquiry-retry-b");
        assertThat(attempt(source, 109L).occurredAt())
                .isEqualTo(attempt(source, 110L).occurredAt());
        assertThat(source.attempts()).filteredOn(
                        row -> row.requestId().equals("inquiry-direct"))
                .extracting(RawAttempt::eventType)
                .containsExactlyInAnyOrder(EventType.ISSUE_ATTEMPT, EventType.ISSUE_RESULT);
        assertThat(source.attempts()).extracting(RawAttempt::eventType)
                .doesNotContain(EventType.QUEUE_ADMITTED);
    }

    private static RawAttempt attempt(AdminIssuanceInquirySource source, long attemptId) {
        return source.attempts().stream()
                .filter(row -> row.attemptId() == attemptId)
                .findFirst()
                .orElseThrow();
    }

}

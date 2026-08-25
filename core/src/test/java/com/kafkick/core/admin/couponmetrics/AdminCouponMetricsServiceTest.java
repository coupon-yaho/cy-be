package com.kafkick.core.admin.couponmetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataErrorCode;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.campaignsource.AdminCampaignDetailData;
import com.kafkick.core.admin.campaignsource.DetailAvailability;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

class AdminCouponMetricsServiceTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");

    @Test
    void passesTheExactHalfOpenWindowToTheReaderAndKeepsUnconnectedSourcesPending() {
        RecordingReader reader = new RecordingReader(availableDetail());
        AdminCouponMetricsService service = service(reader);

        CouponMetricsSnapshot result = service.getCouponMetrics(101L, MetricsWindow.FIVE_MINUTES);

        assertThat(reader.calls).isEqualTo(1);
        assertThat(reader.couponId).isEqualTo(101L);
        assertThat(reader.fromInclusive).isEqualTo(Instant.parse("2026-08-21T23:55:00Z"));
        assertThat(reader.toExclusive).isEqualTo(SNAPSHOT_AT);
        assertThat(reader.snapshotAt).isEqualTo(SNAPSHOT_AT);
        assertThat(result.stock().initialCount().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.holdingCounts().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.transitionRate().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.issuanceRate().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.queue().waitingCount().status()).isEqualTo(SourceStatus.PENDING);
    }

    @Test
    void mapsOnlyMissingCampaignToCommonNotFound() {
        AdminCouponMetricsService service = service(new RecordingReader(
                new AdminCampaignDetailData(DetailAvailability.NOT_FOUND, null)));

        assertThatThrownBy(() -> service.getCouponMetrics(404L, MetricsWindow.ONE_MINUTE))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(CommonErrorCode.NOT_FOUND));
    }

    @Test
    void mapsDatabaseFailureToCampaignObservationUnavailable() {
        AdminCouponMetricsService service = service(new RecordingReader(
                new AdminCampaignDetailData(DetailAvailability.UNAVAILABLE, null)));

        assertThatThrownBy(() -> service.getCouponMetrics(101L, MetricsWindow.ONE_MINUTE))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(AdminCampaignDataErrorCode.OBSERVATION_UNAVAILABLE));
    }

    private static AdminCouponMetricsService service(AdminCampaignDataReader reader) {
        return new AdminCouponMetricsService(
                new TimeProvider(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC)), reader,
                new CouponMetricsCalculator());
    }

    private static AdminCampaignDetailData availableDetail() {
        CouponMetricsSource.Observation<CouponMetricsSource.StockCounts> stock =
                new CouponMetricsSource.Observation<>(
                        new CouponMetricsSource.StockCounts(10L, 4L), SourceStatus.VALID, SNAPSHOT_AT);
        CouponMetricsSource.Observation<CouponMetricsSource.IssuanceStatusCounts> holdings =
                new CouponMetricsSource.Observation<>(
                        new CouponMetricsSource.IssuanceStatusCounts(3L, 1L, 2L, 1L),
                        SourceStatus.VALID, SNAPSHOT_AT);
        CouponMetricsSource.Observation<List<CouponMetricsSource.TransitionBucket>> transitions =
                new CouponMetricsSource.Observation<>(List.of(
                        new CouponMetricsSource.TransitionBucket(
                                SNAPSHOT_AT.minusSeconds(300), SNAPSHOT_AT, 2L, 1L, 1L, 1L)),
                        SourceStatus.VALID, SNAPSHOT_AT);
        return new AdminCampaignDetailData(DetailAvailability.AVAILABLE,
                new AdminCampaignDetailData.DetailValue(
                        101L, "campaign", "brand",
                        new CouponMetricsSource.CampaignRuntime(CouponRoundStatus.OPEN,
                                SNAPSHOT_AT.minusSeconds(60)), stock, holdings, transitions));
    }

    private static final class RecordingReader implements AdminCampaignDataReader {

        private final AdminCampaignDetailData detail;
        private int calls;
        private long couponId;
        private Instant fromInclusive;
        private Instant toExclusive;
        private Instant snapshotAt;

        private RecordingReader(AdminCampaignDetailData detail) {
            this.detail = detail;
        }

        @Override
        public AdminCampaignCatalog loadCatalog(Instant requestedAt) {
            throw new AssertionError("상세 요청에서 catalog를 읽으면 안 됩니다.");
        }

        @Override
        public AdminCampaignDetailData findDetail(
                long requestedCouponId,
                Instant requestedFrom,
                Instant requestedTo,
                Instant requestedSnapshotAt
        ) {
            calls++;
            couponId = requestedCouponId;
            fromInclusive = requestedFrom;
            toExclusive = requestedTo;
            snapshotAt = requestedSnapshotAt;
            return detail;
        }
    }
}

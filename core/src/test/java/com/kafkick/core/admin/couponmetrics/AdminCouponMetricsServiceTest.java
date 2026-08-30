package com.kafkick.core.admin.couponmetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataErrorCode;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDetailData;
import com.kafkick.core.admin.couponroundsource.DetailAvailability;
import com.kafkick.core.admin.queue.PendingAdminQueueObservationSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

class AdminCouponMetricsServiceTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");

    @Test
    void readsOpenCouponRoundRatesAtTheSameSnapshotAsTheDatabaseDetail() {
        RecordingReader reader = new RecordingReader(availableDetail());
        RecordingRateReader rateReader = new RecordingRateReader(observedRates());
        AdminCouponMetricsService service = service(reader, rateReader);

        CouponMetricsSnapshot result = service.getCouponMetrics(101L, MetricsWindow.FIVE_MINUTES);

        assertThat(reader.calls).isEqualTo(1);
        assertThat(reader.couponId).isEqualTo(101L);
        assertThat(reader.fromInclusive).isEqualTo(Instant.parse("2026-08-21T23:55:00Z"));
        assertThat(reader.toExclusive).isEqualTo(SNAPSHOT_AT);
        assertThat(reader.snapshotAt).isEqualTo(SNAPSHOT_AT);
        assertThat(rateReader.calls).isEqualTo(1);
        assertThat(rateReader.couponId).isEqualTo(101L);
        assertThat(rateReader.window).isEqualTo(MetricsWindow.FIVE_MINUTES);
        assertThat(rateReader.snapshotAt).isEqualTo(SNAPSHOT_AT);
        assertThat(result.stock().initialCount().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.holdingCounts().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.transitionRate().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.issuanceRate().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.issuanceRate().value())
                .isEqualTo(new CouponMetricsSnapshot.RateSummary(2.0, 2.0));
        assertThat(result.queue().waitingCount().status()).isEqualTo(SourceStatus.PENDING);
    }

    @Test
    void doesNotQueryRatesForNonOpenCouponRounds() {
        RecordingRateReader rateReader = new RecordingRateReader(observedRates());
        AdminCouponMetricsService service = service(
                new RecordingReader(availableDetail(CouponRoundStatus.SCHEDULED)), rateReader);

        CouponMetricsSnapshot result = service.getCouponMetrics(101L, MetricsWindow.FIVE_MINUTES);

        assertThat(rateReader.calls).isZero();
        assertThat(result.issuanceRate().status()).isEqualTo(SourceStatus.N_A);
        assertThat(result.issuanceRate().value()).isNull();
    }

    @Test
    void preservesDatabaseMetricsWhenRateReaderIsUnavailable() {
        AdminCouponMetricsService service = service(new RecordingReader(availableDetail()),
                new RecordingRateReader(new CouponMetricsSource.Observation<>(
                        null, SourceStatus.UNAVAILABLE, null)));

        CouponMetricsSnapshot result = service.getCouponMetrics(101L, MetricsWindow.FIVE_MINUTES);

        assertThat(result.stock().initialCount().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.holdingCounts().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.transitionRate().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.issuanceRate().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    @Test
    void mapsOnlyMissingCouponRoundToCommonNotFound() {
        AdminCouponMetricsService service = service(new RecordingReader(
                new AdminCouponRoundDetailData(DetailAvailability.NOT_FOUND, null)), new RecordingRateReader(observedRates()));

        assertThatThrownBy(() -> service.getCouponMetrics(404L, MetricsWindow.ONE_MINUTE))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(CommonErrorCode.NOT_FOUND));
    }

    @Test
    void mapsDatabaseFailureToCouponRoundObservationUnavailable() {
        AdminCouponMetricsService service = service(new RecordingReader(
                new AdminCouponRoundDetailData(DetailAvailability.UNAVAILABLE, null)), new RecordingRateReader(observedRates()));

        assertThatThrownBy(() -> service.getCouponMetrics(101L, MetricsWindow.ONE_MINUTE))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(AdminCouponRoundDataErrorCode.OBSERVATION_UNAVAILABLE));
    }

    private static AdminCouponMetricsService service(
            AdminCouponRoundDataReader reader,
            CouponIssuanceRateReader rateReader
    ) {
        return new AdminCouponMetricsService(
                new TimeProvider(Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC)), reader,
                rateReader, new PendingAdminQueueObservationSource(), new CouponMetricsCalculator());
    }

    private static AdminCouponRoundDetailData availableDetail() {
        return availableDetail(CouponRoundStatus.OPEN);
    }

    private static AdminCouponRoundDetailData availableDetail(CouponRoundStatus status) {
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
        return new AdminCouponRoundDetailData(DetailAvailability.AVAILABLE,
                new AdminCouponRoundDetailData.DetailValue(
                        101L, "couponRound", "brand",
                        new CouponMetricsSource.CouponRoundRuntime(status,
                                SNAPSHOT_AT.minusSeconds(60)), stock, holdings, transitions));
    }

    private static CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> observedRates() {
        return new CouponMetricsSource.Observation<>(List.of(
                new CouponMetricsSource.IssuanceRateSample(SNAPSHOT_AT.minusSeconds(5), 2.0),
                new CouponMetricsSource.IssuanceRateSample(SNAPSHOT_AT, 2.0)),
                SourceStatus.VALID, SNAPSHOT_AT);
    }

    private static final class RecordingReader implements AdminCouponRoundDataReader {

        private final AdminCouponRoundDetailData detail;
        private int calls;
        private long couponId;
        private Instant fromInclusive;
        private Instant toExclusive;
        private Instant snapshotAt;

        private RecordingReader(AdminCouponRoundDetailData detail) {
            this.detail = detail;
        }

        @Override
        public AdminCouponRoundCatalog loadCatalog(Instant requestedAt) {
            throw new AssertionError("상세 요청에서 catalog를 읽으면 안 됩니다.");
        }

        @Override
        public AdminCouponRoundDetailData findDetail(
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

    private static final class RecordingRateReader implements CouponIssuanceRateReader {

        private final CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> result;
        private int calls;
        private long couponId;
        private MetricsWindow window;
        private Instant snapshotAt;

        private RecordingRateReader(
                CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> result
        ) {
            this.result = result;
        }

        @Override
        public CouponMetricsSource.Observation<List<CouponMetricsSource.IssuanceRateSample>> read(
                long requestedCouponId,
                MetricsWindow requestedWindow,
                Instant requestedSnapshotAt
        ) {
            calls++;
            couponId = requestedCouponId;
            window = requestedWindow;
            snapshotAt = requestedSnapshotAt;
            return result;
        }
    }
}

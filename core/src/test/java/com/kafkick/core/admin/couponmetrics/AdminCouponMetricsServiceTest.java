package com.kafkick.core.admin.couponmetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.MetricsWindow;
import com.kafkick.core.admin.couponmetrics.mock.AdminCouponMetricsMockDataFactory;
import com.kafkick.core.admin.overview.mock.AdminOverviewMockDataFactory;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

class AdminCouponMetricsServiceTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-22T00:00:00Z");

    @Test
    void readsTimeAndSourceOnceThenCalculatesRequestedWindow() {
        RecordingTimeProvider timeProvider = new RecordingTimeProvider();
        RecordingMockDataFactory factory = new RecordingMockDataFactory();
        AdminCouponMetricsService service = new AdminCouponMetricsService(
                timeProvider, factory, new CouponMetricsCalculator());

        CouponMetricsSnapshot result = service.getCouponMetrics(
                101L, MetricsWindow.FIVE_MINUTES);

        assertThat(result.couponId()).isEqualTo(101L);
        assertThat(result.snapshotAt()).isEqualTo(SNAPSHOT_AT);
        assertThat(result.window()).isEqualTo(MetricsWindow.FIVE_MINUTES);
        assertThat(timeProvider.instantCount).isEqualTo(1);
        assertThat(factory.findCount).isEqualTo(1);
        assertThat(factory.lastSnapshotAt).isEqualTo(SNAPSHOT_AT);
        assertThat(factory.lastCouponId).isEqualTo(101L);
    }

    @Test
    void rejectsUnknownCouponWithCommonNotFound() {
        AdminCouponMetricsService service = service();

        assertThatThrownBy(() -> service.getCouponMetrics(
                999_999L, MetricsWindow.ONE_MINUTE))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(CommonErrorCode.NOT_FOUND));
    }

    private static AdminCouponMetricsService service() {
        TimeProvider timeProvider = new TimeProvider(
                Clock.fixed(SNAPSHOT_AT, ZoneOffset.UTC));
        AdminCouponMetricsMockDataFactory factory = new AdminCouponMetricsMockDataFactory(
                new AdminOverviewMockDataFactory());
        return new AdminCouponMetricsService(
                timeProvider, factory, new CouponMetricsCalculator());
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

    private static final class RecordingMockDataFactory extends AdminCouponMetricsMockDataFactory {

        private int findCount;
        private Instant lastSnapshotAt;
        private long lastCouponId;

        private RecordingMockDataFactory() {
            super(new AdminOverviewMockDataFactory());
        }

        @Override
        public Optional<CouponMetricsSource> find(Instant snapshotAt, long couponId) {
            findCount++;
            lastSnapshotAt = snapshotAt;
            lastCouponId = couponId;
            return super.find(snapshotAt, couponId);
        }
    }
}

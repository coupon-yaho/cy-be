package com.kafkick.core.admin.analytics;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateObservation;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.BrandRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CampaignRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CatalogSnapshot;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.DailyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.HourlyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.IssuanceStatusAggregate;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 브랜드 분석 계산의 기간·빈 버킷·상태 분포 계약을 검증합니다. */
class AdminAnalyticsCalculatorTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-03-11T00:00:00Z");
    private static final Instant EVALUATED_AT = Instant.parse("2026-03-11T00:30:00Z");
    private static final AdminAnalyticsQuery QUERY = new AdminAnalyticsQuery(
            LocalDate.parse("2026-01-15"),
            LocalDate.parse("2026-03-10"),
            null,
            null,
            ZoneId.of("Asia/Seoul"));

    private final AdminAnalyticsCalculator calculator = new AdminAnalyticsCalculator(
            new AdminAnalyticsFreshnessPolicy(Duration.ofHours(1)));

    /** 부분 월을 포함하고 비어 있는 2월을 0으로 채우는지 검증합니다. */
    @Test
    @DisplayName("월별 추이는 요청 범위 안의 월을 브랜드별로 연속 반환한다")
    void fillsMissingMonthsForEachBrand() {
        AdminAnalyticsResult result = calculator.calculate(QUERY, dataset(
                available(List.of(
                        new DailyIssueAggregate(LocalDate.parse("2026-01-15"), 1L, 101L, 12L),
                        new DailyIssueAggregate(LocalDate.parse("2026-03-10"), 1L, 101L, 7L))),
                available(List.of()),
                available(List.of(status(18L, 10L, 4L, 2L, 2L)))), EVALUATED_AT);

        assertThat(result.brandTrends().status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.brandTrends().value())
                .extracting(AdminAnalyticsResult.BrandTrendPoint::periodStart,
                        AdminAnalyticsResult.BrandTrendPoint::issueCount)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(LocalDate.parse("2026-01-01"), 12L),
                        org.assertj.core.groups.Tuple.tuple(LocalDate.parse("2026-02-01"), 0L),
                        org.assertj.core.groups.Tuple.tuple(LocalDate.parse("2026-03-01"), 7L));
    }

    /** 월요일부터 일요일, 각 0시부터 23시까지 고정된 168셀을 생성하는지 검증합니다. */
    @Test
    @DisplayName("히트맵은 ISO 요일과 시간 오름차순의 168개 셀을 반환한다")
    void createsOrderedHeatmapCells() {
        AdminAnalyticsResult result = calculator.calculate(QUERY, dataset(
                available(List.of()),
                available(List.of(new HourlyIssueAggregate(
                        LocalDate.parse("2026-01-19"), 13, 1L, 101L, 5L))),
                available(List.of(status(0L, 0L, 0L, 0L, 0L)))), EVALUATED_AT);

        assertThat(result.hourlyHeatmap().value()).hasSize(168);
        assertThat(result.hourlyHeatmap().value().get(0))
                .isEqualTo(new AdminAnalyticsResult.HourlyHeatmapCell(DayOfWeek.MONDAY, 0, 0L));
        assertThat(result.hourlyHeatmap().value().get(13))
                .isEqualTo(new AdminAnalyticsResult.HourlyHeatmapCell(DayOfWeek.MONDAY, 13, 5L));
        assertThat(result.hourlyHeatmap().value().get(167))
                .isEqualTo(new AdminAnalyticsResult.HourlyHeatmapCell(DayOfWeek.SUNDAY, 23, 0L));
    }

    /** 전체 발급 수와 현재 미사용 발급 수를 섞지 않고 네 상태 비율을 계산하는지 검증합니다. */
    @Test
    @DisplayName("상태 분포는 현재 네 상태를 합산한 뒤 totalIssued 기준 비율을 계산한다")
    void calculatesCurrentStatusDistribution() {
        AdminAnalyticsResult result = calculator.calculate(QUERY, dataset(
                available(List.of()),
                available(List.of()),
                available(List.of(status(20L, 10L, 4L, 3L, 3L)))), EVALUATED_AT);

        AdminAnalyticsResult.IssuanceStatusDistribution distribution =
                result.issuanceStatusDistribution().value();

        assertThat(distribution.totalIssued()).isEqualTo(20L);
        assertThat(distribution.currentlyIssued()).isEqualTo(10L);
        assertThat(distribution.statuses())
                .containsExactly(
                        new AdminAnalyticsResult.StatusCount(IssuanceStatus.ISSUED, 10L, 0.5D),
                        new AdminAnalyticsResult.StatusCount(IssuanceStatus.USED, 4L, 0.2D),
                        new AdminAnalyticsResult.StatusCount(IssuanceStatus.CANCELLED, 3L, 0.15D),
                        new AdminAnalyticsResult.StatusCount(IssuanceStatus.EXPIRED, 3L, 0.15D));
    }

    /** 발급이 한 건도 없을 때 네 상태의 비율을 유한한 0으로 반환하는지 검증합니다. */
    @Test
    @DisplayName("상태 분포는 0분모를 NO_TRAFFIC과 0 비율로 처리한다")
    void handlesZeroDenominatorWithoutNonFiniteRatio() {
        AdminAnalyticsResult result = calculator.calculate(QUERY, dataset(
                available(List.of()),
                available(List.of()),
                available(List.of(status(0L, 0L, 0L, 0L, 0L)))), EVALUATED_AT);

        assertThat(result.issuanceStatusDistribution().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(result.issuanceStatusDistribution().value().statuses())
                .allSatisfy(status -> assertThat(status.ratio()).isZero().isFinite());
    }

    /** 원천별 관측 시각을 독립적으로 판정하되 같은 평가 시각을 사용하는지 검증합니다. */
    @Test
    @DisplayName("분석별 AVAILABLE과 PENDING 및 STALE 상태를 독립적으로 보존한다")
    void preservesIndependentObservationStates() {
        AggregateObservation<List<HourlyIssueAggregate>> oldHeatmap = new AggregateObservation<>(
                List.of(), AggregateAvailability.AVAILABLE, EVALUATED_AT.minus(Duration.ofHours(2)));
        AggregateObservation<List<IssuanceStatusAggregate>> pending = AggregateObservation.pending();

        AdminAnalyticsResult result = calculator.calculate(QUERY, dataset(
                available(List.of()), oldHeatmap, pending), EVALUATED_AT);

        assertThat(result.brandTrends().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(result.hourlyHeatmap().status()).isEqualTo(SourceStatus.STALE);
        assertThat(result.issuanceStatusDistribution().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(result.issuanceStatusDistribution().value()).isNull();
    }

    /** 원천 조회 장애를 PENDING이나 정상 0건으로 바꾸지 않는지 검증합니다. */
    @Test
    @DisplayName("UNAVAILABLE 분석은 값과 관측 시각 없이 그대로 보존한다")
    void preservesUnavailableWithoutSyntheticValue() {
        AdminAnalyticsResult result = calculator.calculate(QUERY, dataset(
                AggregateObservation.unavailable(),
                available(List.of()),
                available(List.of(status(0L, 0L, 0L, 0L, 0L)))), EVALUATED_AT);

        assertThat(result.brandTrends().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.brandTrends().value()).isNull();
        assertThat(result.brandTrends().observedAt()).isNull();
    }

    private static AdminAnalyticsDataset dataset(
            AggregateObservation<List<DailyIssueAggregate>> daily,
            AggregateObservation<List<HourlyIssueAggregate>> hourly,
            AggregateObservation<List<IssuanceStatusAggregate>> statuses
    ) {
        return new AdminAnalyticsDataset(
                AnalyticsSourceType.MOCK,
                new CatalogSnapshot(
                        AggregateAvailability.AVAILABLE,
                        List.of(new BrandRef(1L, "브랜드")),
                        List.of(new CampaignRef(
                                101L,
                                1L,
                                LocalDate.parse("2025-12-01"),
                                LocalDate.parse("2026-12-31")))),
                daily,
                hourly,
                statuses);
    }

    private static IssuanceStatusAggregate status(
            long total,
            long issued,
            long used,
            long cancelled,
            long expired
    ) {
        return new IssuanceStatusAggregate(
                1L,
                101L,
                QUERY.from(),
                QUERY.to(),
                total,
                issued,
                used,
                cancelled,
                expired);
    }

    private static <T> AggregateObservation<T> available(T value) {
        return new AggregateObservation<>(value, AggregateAvailability.AVAILABLE, OBSERVED_AT);
    }
}

package com.kafkick.core.admin.analytics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import com.kafkick.core.support.exception.BusinessException;

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
                List.of(new HourlyIssueAggregate(
                        LocalDate.parse("2026-01-19"), 13, 1L, 101L, 5L)),
                AggregateAvailability.AVAILABLE,
                EVALUATED_AT.minus(Duration.ofHours(2)));
        AggregateObservation<List<IssuanceStatusAggregate>> pending = AggregateObservation.pending();

        AdminAnalyticsResult result = calculator.calculate(QUERY, dataset(
                available(List.of()), oldHeatmap, pending), EVALUATED_AT);

        assertThat(result.brandTrends().status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(result.hourlyHeatmap().status()).isEqualTo(SourceStatus.STALE);
        assertThat(result.hourlyHeatmap().value())
                .containsExactly(new AdminAnalyticsResult.HourlyHeatmapCell(
                        DayOfWeek.MONDAY, 13, 5L));
        assertThat(result.hourlyHeatmap().observedAt())
                .isEqualTo(EVALUATED_AT.minus(Duration.ofHours(2)));
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

    /** 브랜드와 캠페인 필터가 세 분석 원천에 같은 기준으로 적용되는지 검증합니다. */
    @Test
    @DisplayName("브랜드와 캠페인 필터는 선택한 캠페인의 집계만 계산한다")
    void appliesBrandAndCouponFilters() {
        AdminAnalyticsQuery filteredQuery = new AdminAnalyticsQuery(
                QUERY.from(), QUERY.to(), 1L, 101L, QUERY.zoneId());
        AdminAnalyticsDataset dataset = twoCampaignDataset(
                available(List.of(
                        new DailyIssueAggregate(LocalDate.parse("2026-01-15"), 1L, 101L, 12L),
                        new DailyIssueAggregate(LocalDate.parse("2026-01-15"), 2L, 102L, 8L))),
                available(List.of(
                        new HourlyIssueAggregate(LocalDate.parse("2026-01-19"), 13, 1L, 101L, 5L),
                        new HourlyIssueAggregate(LocalDate.parse("2026-01-20"), 18, 2L, 102L, 8L))),
                available(List.of(
                        status(1L, 101L, filteredQuery, 12L, 6L, 3L, 2L, 1L),
                        status(2L, 102L, filteredQuery, 8L, 3L, 2L, 2L, 1L))));

        AdminAnalyticsResult result = calculator.calculate(filteredQuery, dataset, EVALUATED_AT);

        assertThat(result.brands()).extracting(BrandRef::brandId).containsExactly(1L);
        assertThat(result.brandTrends().value())
                .extracting(AdminAnalyticsResult.BrandTrendPoint::brandId)
                .containsOnly(1L);
        assertThat(result.hourlyHeatmap().value())
                .filteredOn(cell -> cell.issueCount() > 0L)
                .containsExactly(new AdminAnalyticsResult.HourlyHeatmapCell(
                        DayOfWeek.MONDAY, 13, 5L));
        assertThat(result.issuanceStatusDistribution().value().totalIssued()).isEqualTo(12L);
    }

    /** 브랜드와 캠페인 필터가 각각 단독으로도 모집단을 제한하는지 검증합니다. */
    @Test
    @DisplayName("브랜드와 캠페인 단독 필터는 각각 해당 모집단만 반환한다")
    void appliesBrandAndCouponFiltersIndependently() {
        AdminAnalyticsDataset dataset = twoCampaignDataset(
                available(List.of(
                        new DailyIssueAggregate(LocalDate.parse("2026-01-15"), 1L, 101L, 12L),
                        new DailyIssueAggregate(LocalDate.parse("2026-01-15"), 2L, 102L, 8L))),
                available(List.of()),
                available(List.of(
                        status(1L, 101L, QUERY, 12L, 6L, 3L, 2L, 1L),
                        status(2L, 102L, QUERY, 8L, 3L, 2L, 2L, 1L))));

        AdminAnalyticsResult brandResult = calculator.calculate(
                new AdminAnalyticsQuery(
                        QUERY.from(), QUERY.to(), 1L, null, QUERY.zoneId()),
                dataset,
                EVALUATED_AT);
        AdminAnalyticsResult couponResult = calculator.calculate(
                new AdminAnalyticsQuery(
                        QUERY.from(), QUERY.to(), null, 102L, QUERY.zoneId()),
                dataset,
                EVALUATED_AT);

        assertThat(brandResult.brands()).extracting(BrandRef::brandId).containsExactly(1L);
        assertThat(brandResult.issuanceStatusDistribution().value().totalIssued()).isEqualTo(12L);
        assertThat(couponResult.brands()).extracting(BrandRef::brandId).containsExactly(2L);
        assertThat(couponResult.issuanceStatusDistribution().value().totalIssued()).isEqualTo(8L);
    }

    /** 선택하지 않은 캠페인의 기간 계약은 현재 요청 계산에 영향을 주지 않는지 검증합니다. */
    @Test
    @DisplayName("상태 집계 기간은 요청 필터를 통과한 행에만 검증한다")
    void validatesStatusWindowAfterQueryFilter() {
        AdminAnalyticsQuery filteredQuery = new AdminAnalyticsQuery(
                QUERY.from(), QUERY.to(), 1L, 101L, QUERY.zoneId());
        AdminAnalyticsDataset dataset = twoCampaignDataset(
                available(List.of()),
                available(List.of()),
                available(List.of(
                        status(1L, 101L, filteredQuery, 12L, 6L, 3L, 2L, 1L),
                        status(2L, 102L, new AdminAnalyticsQuery(
                                LocalDate.parse("2025-01-01"),
                                LocalDate.parse("2025-01-31"), null, null, QUERY.zoneId()),
                                8L, 3L, 2L, 2L, 1L))));

        AdminAnalyticsResult result = calculator.calculate(filteredQuery, dataset, EVALUATED_AT);

        assertThat(result.issuanceStatusDistribution().value().totalIssued()).isEqualTo(12L);
    }

    /** 선택된 상태 집계 행의 기간이 요청과 다르면 안정적인 분석 오류를 반환하는지 검증합니다. */
    @Test
    @DisplayName("선택된 상태 집계 기간이 요청과 다르면 원천 계약 오류로 거부한다")
    void rejectsSelectedStatusWindowMismatch() {
        AdminAnalyticsQuery otherWindow = new AdminAnalyticsQuery(
                LocalDate.parse("2025-01-01"),
                LocalDate.parse("2025-01-31"),
                null,
                null,
                QUERY.zoneId());
        AdminAnalyticsDataset dataset = dataset(
                available(List.of()),
                available(List.of()),
                available(List.of(status(
                        1L, 101L, otherWindow, 8L, 3L, 2L, 2L, 1L))));

        assertThatThrownBy(() -> calculator.calculate(QUERY, dataset, EVALUATED_AT))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("ANALYTICS-001");
    }

    /** 응답 모집단 밖 집계 행이 정상 합계에만 반영되는 모순을 차단하는지 검증합니다. */
    @Test
    @DisplayName("월별 집계 행의 브랜드가 응답 모집단 밖이면 원천 계약 오류로 거부한다")
    void rejectsMonthlyAggregateOutsideResponsePopulation() {
        AdminAnalyticsDataset dataset = new AdminAnalyticsDataset(
                AnalyticsSourceType.MOCK,
                new CatalogSnapshot(
                        AggregateAvailability.AVAILABLE,
                        List.of(new BrandRef(1L, "브랜드"), new BrandRef(2L, "기간 밖 브랜드")),
                        List.of(
                                new CampaignRef(101L, 1L,
                                        LocalDate.parse("2025-12-01"), LocalDate.parse("2026-12-31")),
                                new CampaignRef(102L, 2L,
                                        LocalDate.parse("2025-01-01"), LocalDate.parse("2025-12-31")))),
                available(List.of(new DailyIssueAggregate(
                        LocalDate.parse("2026-01-15"), 2L, 102L, 8L))),
                available(List.of()),
                available(List.of()));

        assertThatThrownBy(() -> calculator.calculate(QUERY, dataset, EVALUATED_AT))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("ANALYTICS-001");
    }

    /** 월별 값이 없어도 시간 집계가 응답 모집단 밖 브랜드를 포함하지 못하게 하는지 검증합니다. */
    @Test
    @DisplayName("월별 원천이 PENDING이어도 시간 집계의 모집단 밖 브랜드를 거부한다")
    void rejectsHourlyAggregateOutsideResponsePopulationWhenMonthlyPending() {
        AdminAnalyticsDataset dataset = outsidePopulationDataset(
                AggregateObservation.pending(),
                available(List.of(new HourlyIssueAggregate(
                        LocalDate.parse("2026-01-19"), 13, 2L, 102L, 8L))),
                available(List.of()));

        assertThatThrownBy(() -> calculator.calculate(QUERY, dataset, EVALUATED_AT))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("ANALYTICS-001");
    }

    /** 월별 값이 없어도 상태 집계가 응답 모집단 밖 브랜드를 포함하지 못하게 하는지 검증합니다. */
    @Test
    @DisplayName("월별 원천이 UNAVAILABLE이어도 상태 집계의 모집단 밖 브랜드를 거부한다")
    void rejectsStatusAggregateOutsideResponsePopulationWhenMonthlyUnavailable() {
        AdminAnalyticsDataset dataset = outsidePopulationDataset(
                AggregateObservation.unavailable(),
                AggregateObservation.pending(),
                available(List.of(status(2L, 102L, QUERY, 8L, 3L, 2L, 2L, 1L))));

        assertThatThrownBy(() -> calculator.calculate(QUERY, dataset, EVALUATED_AT))
                .isInstanceOf(BusinessException.class)
                .extracting(exception -> ((BusinessException) exception).getErrorCode().getCode())
                .isEqualTo("ANALYTICS-001");
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

    /** 필터 경계 테스트에 사용할 두 브랜드·캠페인 Dataset을 만듭니다. */
    private static AdminAnalyticsDataset twoCampaignDataset(
            AggregateObservation<List<DailyIssueAggregate>> daily,
            AggregateObservation<List<HourlyIssueAggregate>> hourly,
            AggregateObservation<List<IssuanceStatusAggregate>> statuses
    ) {
        return new AdminAnalyticsDataset(
                AnalyticsSourceType.MOCK,
                new CatalogSnapshot(
                        AggregateAvailability.AVAILABLE,
                        List.of(new BrandRef(1L, "브랜드1"), new BrandRef(2L, "브랜드2")),
                        List.of(
                                new CampaignRef(101L, 1L,
                                        LocalDate.parse("2025-12-01"), LocalDate.parse("2026-12-31")),
                                new CampaignRef(102L, 2L,
                                        LocalDate.parse("2025-12-01"), LocalDate.parse("2026-12-31")))),
                daily,
                hourly,
                statuses);
    }

    /** 응답 기간과 겹치지 않는 두 번째 브랜드를 포함한 원천 계약 검증용 Dataset을 만듭니다. */
    private static AdminAnalyticsDataset outsidePopulationDataset(
            AggregateObservation<List<DailyIssueAggregate>> daily,
            AggregateObservation<List<HourlyIssueAggregate>> hourly,
            AggregateObservation<List<IssuanceStatusAggregate>> statuses
    ) {
        return new AdminAnalyticsDataset(
                AnalyticsSourceType.MOCK,
                new CatalogSnapshot(
                        AggregateAvailability.AVAILABLE,
                        List.of(new BrandRef(1L, "브랜드"), new BrandRef(2L, "기간 밖 브랜드")),
                        List.of(
                                new CampaignRef(101L, 1L,
                                        LocalDate.parse("2025-12-01"), LocalDate.parse("2026-12-31")),
                                new CampaignRef(102L, 2L,
                                        LocalDate.parse("2025-01-01"), LocalDate.parse("2025-12-31")))),
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
        return status(1L, 101L, QUERY, total, issued, used, cancelled, expired);
    }

    /** 임의의 브랜드·캠페인·기간에 대한 상태 집계 행을 만듭니다. */
    private static IssuanceStatusAggregate status(
            long brandId,
            long couponId,
            AdminAnalyticsQuery query,
            long total,
            long issued,
            long used,
            long cancelled,
            long expired
    ) {
        return new IssuanceStatusAggregate(
                brandId, couponId, query.from(), query.to(), total, issued, used, cancelled, expired);
    }

    private static <T> AggregateObservation<T> available(T value) {
        return new AggregateObservation<>(value, AggregateAvailability.AVAILABLE, OBSERVED_AT);
    }
}

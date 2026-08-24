package com.kafkick.core.admin.analytics.mock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.DailyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.HourlyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.IssuanceStatusAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsQuery;

/** Mock Source가 최종 차트가 아닌 조회 조건별 집계 행을 제공하는지 검증합니다. */
class AdminAnalyticsMockSourceTest {

    private static final Instant OBSERVED_AT = Instant.parse("2026-08-23T02:00:00Z");

    /** 기간·브랜드·캠페인 필터가 집계 행에 적용되고 검증 메타데이터는 보존되는지 확인합니다. */
    @Test
    @DisplayName("Mock Source는 load query에 맞는 원천 집계 행만 반환한다")
    void filtersRawAggregatesByQuery() {
        AdminAnalyticsMockSource source = new AdminAnalyticsMockSource(
                new AdminAnalyticsMockDataFactory(), OBSERVED_AT);
        AdminAnalyticsQuery query = new AdminAnalyticsQuery(
                LocalDate.parse("2026-01-01"),
                LocalDate.parse("2026-01-31"),
                1L,
                101L,
                ZoneId.of("Asia/Seoul"));

        AdminAnalyticsDataset dataset = source.load(query);

        assertThat(dataset.sourceType()).isEqualTo(AnalyticsSourceType.MOCK);
        assertThat(dataset.catalog().brands()).extracting("brandId").containsExactly(1L);
        assertThat(dataset.catalog().campaigns()).extracting("couponId").containsExactly(101L);
        assertThat(dataset.monthlyTrend().value())
                .allMatch(row -> row.brandId() == 1L
                        && row.couponId() == 101L
                        && !row.date().isBefore(query.from())
                        && !row.date().isAfter(query.to()));
        assertThat(dataset.issuanceStatuses().value())
                .allMatch(row -> row.windowFrom().equals(query.from())
                        && row.windowTo().equals(query.to()));
        assertThat(dataset.issuanceStatuses().value())
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.totalIssued()).isEqualTo(12L);
                    assertThat(row.currentlyIssued()).isEqualTo(6L);
                    assertThat(row.used()).isEqualTo(3L);
                    assertThat(row.cancelled()).isEqualTo(2L);
                    assertThat(row.expired()).isEqualTo(1L);
                });
    }

    /** 발급 원천 행이 없는 기간에는 과거 고정 수량을 재사용하지 않는지 검증합니다. */
    @Test
    @DisplayName("Mock Source는 조회 기간에 발급된 쿠폰이 없으면 상태 분포 수량을 0으로 반환한다")
    void returnsZeroStatusCountsForEmptyPeriod() {
        AdminAnalyticsMockSource source = new AdminAnalyticsMockSource(
                new AdminAnalyticsMockDataFactory(), OBSERVED_AT);
        AdminAnalyticsQuery query = new AdminAnalyticsQuery(
                LocalDate.parse("2026-07-01"),
                LocalDate.parse("2026-07-31"),
                1L,
                101L,
                ZoneId.of("Asia/Seoul"));

        AdminAnalyticsDataset dataset = source.load(query);

        assertThat(dataset.issuanceStatuses().value())
                .singleElement()
                .satisfies(row -> {
                    assertThat(row.totalIssued()).isZero();
                    assertThat(row.currentlyIssued()).isZero();
                    assertThat(row.used()).isZero();
                    assertThat(row.cancelled()).isZero();
                    assertThat(row.expired()).isZero();
                });
    }

    /** 같은 발급 모집단을 나타내는 세 원천이 기간별 정답과 캠페인별 합계를 보존하는지 검증합니다. */
    @Test
    @DisplayName("Mock Source의 월별·시간대·상태 발급 모집단은 기간별 정답과 일치한다")
    void keepsAllIssuanceCohortsConsistentForEachPeriod() {
        AdminAnalyticsMockSource source = new AdminAnalyticsMockSource(
                new AdminAnalyticsMockDataFactory(), OBSERVED_AT);
        CampaignKey campaign101 = new CampaignKey(1L, 101L);
        CampaignKey campaign102 = new CampaignKey(2L, 102L);
        List<ExpectedPeriod> periods = List.of(
                new ExpectedPeriod(
                        "2026-01-01", "2026-01-31", Map.of(campaign101, 12L, campaign102, 8L)),
                new ExpectedPeriod(
                        "2026-02-01", "2026-02-28", Map.of(campaign101, 15L)),
                new ExpectedPeriod(
                        "2026-03-01", "2026-03-31", Map.of(campaign101, 7L)),
                new ExpectedPeriod(
                        "2026-01-01", "2026-03-31", Map.of(campaign101, 34L, campaign102, 8L)));

        for (ExpectedPeriod period : periods) {
            AdminAnalyticsQuery query = query(period.from(), period.to());
            AdminAnalyticsDataset dataset = source.load(query);

            assertThat(monthlyTotals(dataset))
                    .as("월별 조회 기간 %s부터 %s까지", query.from(), query.to())
                    .isEqualTo(period.campaignTotals());
            assertThat(hourlyTotals(dataset))
                    .as("시간대 조회 기간 %s부터 %s까지", query.from(), query.to())
                    .isEqualTo(period.campaignTotals());
            assertThat(statusTotals(dataset))
                    .as("상태 조회 기간 %s부터 %s까지", query.from(), query.to())
                    .isEqualTo(period.campaignTotals());
        }
    }

    /** 날짜별 발급 행을 브랜드·캠페인 단위로 합산합니다. */
    private static Map<CampaignKey, Long> monthlyTotals(AdminAnalyticsDataset dataset) {
        return dataset.monthlyTrend().value().stream()
                .collect(Collectors.groupingBy(
                        row -> campaignKey(row.brandId(), row.couponId()),
                        Collectors.summingLong(DailyIssueAggregate::issueCount)));
    }

    /** 시간대별 발급 행을 브랜드·캠페인 단위로 합산합니다. */
    private static Map<CampaignKey, Long> hourlyTotals(AdminAnalyticsDataset dataset) {
        return dataset.hourlyHeatmap().value().stream()
                .collect(Collectors.groupingBy(
                        row -> campaignKey(row.brandId(), row.couponId()),
                        Collectors.summingLong(HourlyIssueAggregate::issueCount)));
    }

    /** 상태 분포 행을 실제 발급이 존재하는 브랜드·캠페인 단위로 합산합니다. */
    private static Map<CampaignKey, Long> statusTotals(AdminAnalyticsDataset dataset) {
        return dataset.issuanceStatuses().value().stream()
                .filter(row -> row.totalIssued() > 0L)
                .collect(Collectors.groupingBy(
                        row -> campaignKey(row.brandId(), row.couponId()),
                        Collectors.summingLong(IssuanceStatusAggregate::totalIssued)));
    }

    /** 세 Mock 원천이 공유할 브랜드·캠페인 집계 키를 만듭니다. */
    private static CampaignKey campaignKey(long brandId, long couponId) {
        return new CampaignKey(brandId, couponId);
    }

    /** 기간별 모집단 일치 검증에 사용할 전체 브랜드 조회 조건을 만듭니다. */
    private static AdminAnalyticsQuery query(String from, String to) {
        return new AdminAnalyticsQuery(
                LocalDate.parse(from),
                LocalDate.parse(to),
                null,
                null,
                ZoneId.of("Asia/Seoul"));
    }

    /** 한 조회 기간에 기대하는 캠페인별 발급 총계입니다. */
    private record ExpectedPeriod(String from, String to, Map<CampaignKey, Long> campaignTotals) { }

    /** 브랜드와 캠페인을 함께 식별하는 테스트 집계 키입니다. */
    private record CampaignKey(long brandId, long couponId) { }
}

package com.kafkick.core.admin.analytics.mock;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateObservation;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.BrandRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CampaignRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CatalogSnapshot;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.DailyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.HourlyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.IssuanceStatusAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsQuery;

/** 실제 집계 Repository가 반환할 형태의 최소 브랜드 분석 Mock 행을 생성합니다. */
public class AdminAnalyticsMockDataFactory {

    private static final List<BrandRef> BRANDS = List.of(
            new BrandRef(1L, "북스토리"),
            new BrandRef(2L, "델리베리"));

    private static final List<CampaignRef> CAMPAIGNS = List.of(
            new CampaignRef(101L, 1L, LocalDate.parse("2025-12-01"), LocalDate.parse("2026-12-31")),
            new CampaignRef(102L, 2L, LocalDate.parse("2026-01-01"), LocalDate.parse("2026-06-30")));

    private static final List<DailyIssueAggregate> DAILY = List.of(
            new DailyIssueAggregate(LocalDate.parse("2026-01-05"), 1L, 101L, 12L),
            new DailyIssueAggregate(LocalDate.parse("2026-01-06"), 2L, 102L, 8L),
            new DailyIssueAggregate(LocalDate.parse("2026-02-10"), 1L, 101L, 15L),
            new DailyIssueAggregate(LocalDate.parse("2026-03-03"), 1L, 101L, 7L));

    private static final List<HourlyIssueAggregate> HOURLY = List.of(
            new HourlyIssueAggregate(LocalDate.parse("2026-01-05"), 9, 1L, 101L, 5L),
            new HourlyIssueAggregate(LocalDate.parse("2026-01-05"), 13, 1L, 101L, 7L),
            new HourlyIssueAggregate(LocalDate.parse("2026-01-06"), 18, 2L, 102L, 8L),
            new HourlyIssueAggregate(LocalDate.parse("2026-02-10"), 20, 1L, 101L, 15L));

    /** 요청 조건에 맞는 검증 메타데이터와 원천 집계 행을 생성합니다. */
    public AdminAnalyticsDataset create(AdminAnalyticsQuery query, Instant observedAt) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(observedAt, "observedAt");
        List<BrandRef> brands = selectedBrands(query);
        List<CampaignRef> campaigns = selectedCampaigns(query);
        List<DailyIssueAggregate> daily = DAILY.stream()
                .filter(row -> inRange(row.date(), query))
                .filter(row -> matches(row.brandId(), row.couponId(), query))
                .toList();
        List<HourlyIssueAggregate> hourly = HOURLY.stream()
                .filter(row -> inRange(row.date(), query))
                .filter(row -> matches(row.brandId(), row.couponId(), query))
                .toList();
        List<IssuanceStatusAggregate> statuses = statusRows(query);

        return new AdminAnalyticsDataset(
                AnalyticsSourceType.MOCK,
                new CatalogSnapshot(AggregateAvailability.AVAILABLE, brands, campaigns),
                available(daily, observedAt),
                available(hourly, observedAt),
                available(statuses, observedAt));
    }

    /** 요청한 브랜드는 기간 중 캠페인이 없어도 존재 검증을 위해 보존합니다. */
    private static List<BrandRef> selectedBrands(AdminAnalyticsQuery query) {
        if (query.brandId() != null || query.couponId() != null) {
            Set<Long> requiredBrandIds = new HashSet<>();
            if (query.brandId() != null) {
                requiredBrandIds.add(query.brandId());
            }
            CAMPAIGNS.stream()
                    .filter(campaign -> query.couponId() != null
                            && campaign.couponId() == query.couponId())
                    .map(CampaignRef::brandId)
                    .forEach(requiredBrandIds::add);
            // 소속 불일치도 Service가 404로 판정할 수 있도록 요청 브랜드와 실제 소유 브랜드를 함께 보존합니다.
            return BRANDS.stream()
                    .filter(brand -> requiredBrandIds.contains(brand.brandId()))
                    .toList();
        }
        return BRANDS.stream()
                .filter(brand -> CAMPAIGNS.stream().anyMatch(campaign ->
                        campaign.brandId() == brand.brandId()
                                && campaign.overlaps(query.from(), query.to())))
                .toList();
    }

    /** 요청한 캠페인은 기간과 겹치지 않아도 존재·소속 검증을 위해 보존합니다. */
    private static List<CampaignRef> selectedCampaigns(AdminAnalyticsQuery query) {
        if (query.couponId() != null) {
            return CAMPAIGNS.stream()
                    .filter(campaign -> campaign.couponId() == query.couponId())
                    .toList();
        }
        return CAMPAIGNS.stream()
                .filter(campaign -> query.brandId() == null || campaign.brandId() == query.brandId())
                .filter(campaign -> campaign.overlaps(query.from(), query.to()))
                .toList();
    }

    /** 조회 기간 모집단을 흉내 내는 캠페인별 현재 상태 집계 행을 만듭니다. */
    private static List<IssuanceStatusAggregate> statusRows(AdminAnalyticsQuery query) {
        return CAMPAIGNS.stream()
                .filter(campaign -> campaign.overlaps(query.from(), query.to()))
                .filter(campaign -> matches(campaign.brandId(), campaign.couponId(), query))
                .map(campaign -> campaign.couponId() == 101L
                        ? new IssuanceStatusAggregate(
                                1L, 101L, query.from(), query.to(), 20L, 10L, 4L, 3L, 3L)
                        : new IssuanceStatusAggregate(
                                2L, 102L, query.from(), query.to(), 10L, 3L, 3L, 2L, 2L))
                .toList();
    }

    /** 값과 실제 집계 시각을 가진 가용 Observation을 만듭니다. */
    private static <T> AggregateObservation<T> available(T value, Instant observedAt) {
        return new AggregateObservation<>(value, AggregateAvailability.AVAILABLE, observedAt);
    }

    /** 행 날짜가 요청 양끝 포함 범위에 있는지 확인합니다. */
    private static boolean inRange(LocalDate date, AdminAnalyticsQuery query) {
        return !date.isBefore(query.from()) && !date.isAfter(query.to());
    }

    /** 브랜드와 캠페인 선택 필터를 모두 적용합니다. */
    private static boolean matches(long brandId, long couponId, AdminAnalyticsQuery query) {
        return (query.brandId() == null || query.brandId() == brandId)
                && (query.couponId() == null || query.couponId() == couponId);
    }
}

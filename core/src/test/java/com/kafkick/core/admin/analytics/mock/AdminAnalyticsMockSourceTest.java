package com.kafkick.core.admin.analytics.mock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
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
    }
}

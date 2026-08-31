package com.kafkick.core.admin.analytics;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateAvailability;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateObservation;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.BrandRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CouponRoundRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.CatalogSnapshot;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.DailyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.HourlyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.IssuanceStatusAggregate;

/** 분석 원천 계약이 잘못된 값·시각·수량을 계산기 진입 전에 차단하는지 검증합니다. */
class AdminAnalyticsDatasetTest {

    /** 미집계 상태가 가짜 값이나 관측 시각을 보유하지 못하는지 검증합니다. */
    @Test
    @DisplayName("PENDING 원천은 값과 observedAt을 가질 수 없다")
    void pendingRejectsValueAndObservedAt() {
        assertThatThrownBy(() -> new AggregateObservation<>(
                List.of(), AggregateAvailability.PENDING, Instant.parse("2026-01-01T00:00:00Z")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 잘못된 시간대 버킷이 168셀 계약에 들어가지 못하도록 차단하는지 검증합니다. */
    @Test
    @DisplayName("시간 집계 행은 0부터 23까지만 허용한다")
    void hourlyAggregateRejectsInvalidHour() {
        assertThatThrownBy(() -> new HourlyIssueAggregate(
                LocalDate.parse("2026-01-01"), 24, 1L, 1L, 1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 네 현재 상태 합계와 기간 내 전체 발급 수가 달라지는 집계를 거부하는지 검증합니다. */
    @Test
    @DisplayName("상태 집계는 네 상태 합계가 totalIssued와 같아야 한다")
    void statusAggregateRejectsBrokenConservation() {
        assertThatThrownBy(() -> new IssuanceStatusAggregate(
                1L,
                101L,
                LocalDate.parse("2026-01-01"),
                LocalDate.parse("2026-01-31"),
                10L,
                5L,
                2L,
                1L,
                1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("totalIssued");
    }

    /** 집계 행이 카탈로그와 다른 브랜드 소속을 주장하면 계산 전에 거부하는지 검증합니다. */
    @Test
    @DisplayName("AVAILABLE 집계 행은 카탈로그의 쿠폰 회차 소속과 일치해야 한다")
    void availableAggregateRejectsUnknownCatalogReference() {
        Instant observedAt = Instant.parse("2026-01-02T00:00:00Z");
        CatalogSnapshot catalog = new CatalogSnapshot(
                AggregateAvailability.AVAILABLE,
                List.of(new BrandRef(1L, "브랜드")),
                List.of(new CouponRoundRef(
                        101L, 1L, LocalDate.parse("2026-01-01"), LocalDate.parse("2026-12-31"))));

        assertThatThrownBy(() -> new AdminAnalyticsDataset(
                AnalyticsSourceType.AGGREGATE_DB,
                catalog,
                new AggregateObservation<>(
                        List.of(new DailyIssueAggregate(
                                LocalDate.parse("2026-01-01"), 2L, 101L, 1L)),
                        AggregateAvailability.AVAILABLE,
                        observedAt),
                new AggregateObservation<>(List.of(), AggregateAvailability.AVAILABLE, observedAt),
                new AggregateObservation<>(List.of(), AggregateAvailability.AVAILABLE, observedAt)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("카탈로그");
    }
}

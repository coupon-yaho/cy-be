package com.kafkick.api.admin.dashboard.dto;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult.Observation;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.observation.SourceStatus;

/** 지정 기간의 브랜드 발급 추이·히트맵·현재 상태 분포를 독립 관측값으로 반환합니다. */
public record AdminAnalyticsResponse(
        TimeRange range,
        AnalyticsFilterSummary filters,
        AnalyticsSourceType sourceType,
        List<BrandSummary> brands,
        ObservedValue<List<BrandTrendPoint>> brandTrends,
        ObservedValue<List<HourlyHeatmapCell>> hourlyHeatmap,
        ObservedValue<IssuanceStatusDistribution> issuanceStatusDistribution
) {

    /** Core 계산 결과를 HTTP 응답 계약으로 변환합니다. */
    public static AdminAnalyticsResponse from(AdminAnalyticsResult result) {
        return new AdminAnalyticsResponse(
                new TimeRange(result.query().from(), result.query().to()),
                new AnalyticsFilterSummary(result.query().brandId(), result.query().couponId()),
                result.sourceType(),
                result.brands().stream()
                        .map(brand -> new BrandSummary(brand.brandId(), brand.brandName()))
                        .toList(),
                mapTrendObservation(result.brandTrends()),
                mapHeatmapObservation(result.hourlyHeatmap()),
                mapStatusObservation(result.issuanceStatusDistribution()));
    }

    /** 선구축 JSON 직렬화 테스트에서 사용할 값 없는 안전한 응답을 만듭니다. */
    public static AdminAnalyticsResponse draft() {
        return new AdminAnalyticsResponse(
                new TimeRange(null, null),
                new AnalyticsFilterSummary(null, null),
                AnalyticsSourceType.NONE,
                List.of(),
                pending(), pending(), pending());
    }

    /** Core 월별 추이와 상태·관측 시각을 함께 HTTP 값으로 변환합니다. */
    private static ObservedValue<List<BrandTrendPoint>> mapTrendObservation(
            Observation<List<AdminAnalyticsResult.BrandTrendPoint>> observation
    ) {
        List<BrandTrendPoint> value = observation.value() == null ? null : observation.value().stream()
                .map(point -> new BrandTrendPoint(
                        point.periodStart(), point.brandId(), point.issueCount()))
                .toList();
        return observed(value, observation.status(), observation.observedAt());
    }

    /** Core 히트맵의 ISO 요일·시간 셀과 관측 상태를 HTTP 값으로 변환합니다. */
    private static ObservedValue<List<HourlyHeatmapCell>> mapHeatmapObservation(
            Observation<List<AdminAnalyticsResult.HourlyHeatmapCell>> observation
    ) {
        List<HourlyHeatmapCell> value = observation.value() == null ? null : observation.value().stream()
                .map(cell -> new HourlyHeatmapCell(
                        cell.dayOfWeek().getValue(), cell.hour(), cell.issueCount()))
                .toList();
        return observed(value, observation.status(), observation.observedAt());
    }

    /** Core 현재 상태 분포의 전체·미사용 수량과 상태별 비율을 HTTP 값으로 변환합니다. */
    private static ObservedValue<IssuanceStatusDistribution> mapStatusObservation(
            Observation<AdminAnalyticsResult.IssuanceStatusDistribution> observation
    ) {
        AdminAnalyticsResult.IssuanceStatusDistribution source = observation.value();
        IssuanceStatusDistribution value = source == null ? null : new IssuanceStatusDistribution(
                source.totalIssued(),
                source.currentlyIssued(),
                source.statuses().stream()
                        .map(status -> new StatusCount(
                                status.status(), status.count(), status.ratio()))
                        .toList());
        return observed(value, observation.status(), observation.observedAt());
    }

    /** Core의 값 보유 규칙을 공통 관리자 HTTP Wrapper에 그대로 적용합니다. */
    private static <T> ObservedValue<T> observed(
            T value,
            SourceStatus status,
            Instant observedAt
    ) {
        return new ObservedValue<>(value, status, observedAt);
    }

    /** 아직 집계되지 않은 분석 영역을 값·관측 시각 없는 PENDING으로 만듭니다. */
    private static <T> ObservedValue<T> pending() {
        return new ObservedValue<>(null, SourceStatus.PENDING, null);
    }

    /** 서버가 실제 분석에 적용한 양끝 포함 조회 기간입니다. */
    public record TimeRange(LocalDate from, LocalDate to) { }

    /** 서버가 적용한 선택 브랜드·캠페인 회차 필터입니다. */
    public record AnalyticsFilterSummary(Long brandId, Long couponId) { }

    /** 차트 범례에 사용할 브랜드 식별자와 표시 이름입니다. */
    public record BrandSummary(long brandId, String brandName) { }

    /** 월 시작일 기준 브랜드 발급 합계입니다. */
    public record BrandTrendPoint(LocalDate periodStart, long brandId, long issueCount) { }

    /** ISO-8601 요일 1~7과 0~23시 기준 발급 합계입니다. */
    public record HourlyHeatmapCell(int dayOfWeek, int hour, long issueCount) { }

    /** 기간 내 전체 발급 수와 집계 시점의 현재 상태별 분포입니다. */
    public record IssuanceStatusDistribution(
            long totalIssued,
            long currentlyIssued,
            List<StatusCount> statuses
    ) { }

    /** 하나의 현재 발급 상태에 해당하는 수량과 전체 대비 비율입니다. */
    public record StatusCount(IssuanceStatus status, long count, double ratio) { }
}

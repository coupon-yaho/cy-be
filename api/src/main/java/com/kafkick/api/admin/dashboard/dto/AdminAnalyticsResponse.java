package com.kafkick.api.admin.dashboard.dto;

import java.time.LocalDate;
import java.util.List;

/**
 * 지정 기간의 브랜드 발급 추이, 요일·시간대 히트맵, 발급 퍼널을 한 번에 반환하는 분석 응답 초안입니다.
 *
 * <p>{@code range}와 {@code filters}는 서버가 실제 적용한 조회 조건을 되돌려주며, 각 분석 목록은 데이터가
 * 없을 때 빈 배열을 사용합니다. 퍼널의 {@code stage} 코드 집합과 ratio 계산 규칙은 실제 분석 구현에서
 * 계약을 확정할 수 있도록 현재 String과 double로 열어 둡니다.</p>
 *
 * @param range 서버가 실제 적용한 조회 기간
 * @param filters 서버가 실제 적용한 브랜드·쿠폰 필터
 * @param brandTrends 기간 구간별 브랜드 발급 추이
 * @param hourlyHeatmap 요일·시간대별 발급 건수
 * @param issuanceFunnel 발급 과정 단계별 건수와 비율
 */
public record AdminAnalyticsResponse(TimeRange range, AnalyticsFilterSummary filters, List<BrandTrendPoint> brandTrends,
                                     List<HourlyHeatmapCell> hourlyHeatmap, List<IssuanceFunnelStep> issuanceFunnel) {
    /**
     * 선구축 단계의 JSON 필드 구조를 검증하기 위한 빈 분석 응답 예시를 만듭니다.
     *
     * @return 필터와 분석 목록이 비어 있는 응답 예시
     */
    public static AdminAnalyticsResponse draft() { return new AdminAnalyticsResponse(new TimeRange(null, null), new AnalyticsFilterSummary(null, null), List.of(), List.of(), List.of()); }

    /**
     * 서버가 실제 분석에 적용한 시작일과 종료일입니다.
     *
     * @param from 적용된 시작일
     * @param to 적용된 종료일
     */
    public record TimeRange(LocalDate from, LocalDate to) { }

    /**
     * 선택적으로 적용된 브랜드·쿠폰 필터이며 지정하지 않은 식별자는 null입니다.
     *
     * @param brandId 적용된 브랜드 식별자
     * @param couponId 적용된 쿠폰 캠페인 회차 식별자
     */
    public record AnalyticsFilterSummary(Long brandId, Long couponId) { }

    /**
     * 기간 구간별 브랜드 발급 건수입니다.
     *
     * @param periodStart 집계 구간 시작일
     * @param brandId 브랜드 식별자
     * @param issueCount 해당 구간의 발급 건수
     */
    public record BrandTrendPoint(LocalDate periodStart, Long brandId, long issueCount) { }

    /**
     * 요일과 시간대별 발급 건수이며 시간대 기준은 후속 분석 계약에서 확정합니다.
     *
     * @param dayOfWeek 요일 코드
     * @param hour 0~23 시간대
     * @param issueCount 해당 요일·시간대의 발급 건수
     */
    public record HourlyHeatmapCell(Integer dayOfWeek, Integer hour, long issueCount) { }

    /**
     * 발급 퍼널 한 단계의 건수와 전체 대비 비율입니다.
     *
     * @param stage 퍼널 단계 코드
     * @param count 해당 단계에 도달한 건수
     * @param ratio 기준 단계 대비 비율
     */
    public record IssuanceFunnelStep(FunnelStage stage, long count, double ratio) { }

    /** 분석 화면에서 사용하는 발급 수명주기 단계입니다. */
    public enum FunnelStage { ISSUED, USED, CANCELLED, EXPIRED }
}

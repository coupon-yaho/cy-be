package com.kafkick.core.admin.analytics;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Mock 또는 실제 집계 저장소가 관리자 분석 계산에 제공하는 기술 중립 원천값입니다. */
public record AdminAnalyticsDataset(
        AnalyticsSourceType sourceType,
        CatalogSnapshot catalog,
        AggregateObservation<List<DailyIssueAggregate>> monthlyTrend,
        AggregateObservation<List<HourlyIssueAggregate>> hourlyHeatmap,
        AggregateObservation<List<IssuanceStatusAggregate>> issuanceStatuses
) {

    /** 원천 구분과 카탈로그·분석 세 영역을 검증하고 목록을 불변 복사합니다. */
    public AdminAnalyticsDataset {
        Objects.requireNonNull(sourceType, "sourceType");
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(monthlyTrend, "monthlyTrend");
        Objects.requireNonNull(hourlyHeatmap, "hourlyHeatmap");
        Objects.requireNonNull(issuanceStatuses, "issuanceStatuses");
        monthlyTrend = copyListObservation(monthlyTrend);
        hourlyHeatmap = copyListObservation(hourlyHeatmap);
        issuanceStatuses = copyListObservation(issuanceStatuses);
        validateCatalogReferences(catalog, monthlyTrend, hourlyHeatmap, issuanceStatuses);
    }

    /** 목록 값을 가진 Observation만 불변 복사하고 값 미보유 상태는 그대로 보존합니다. */
    private static <T> AggregateObservation<List<T>> copyListObservation(
            AggregateObservation<List<T>> observation
    ) {
        if (observation.value() == null) {
            return observation;
        }
        return new AggregateObservation<>(
                List.copyOf(observation.value()),
                observation.availability(),
                observation.observedAt());
    }

    /** AVAILABLE 집계 행의 브랜드·캠페인 조합이 함께 받은 카탈로그와 일치하는지 검증합니다. */
    private static void validateCatalogReferences(
            CatalogSnapshot catalog,
            AggregateObservation<List<DailyIssueAggregate>> monthlyTrend,
            AggregateObservation<List<HourlyIssueAggregate>> hourlyHeatmap,
            AggregateObservation<List<IssuanceStatusAggregate>> issuanceStatuses
    ) {
        boolean hasAvailableAggregate = monthlyTrend.availability() == AggregateAvailability.AVAILABLE
                || hourlyHeatmap.availability() == AggregateAvailability.AVAILABLE
                || issuanceStatuses.availability() == AggregateAvailability.AVAILABLE;
        if (!hasAvailableAggregate) {
            return;
        }
        if (catalog.availability() != AggregateAvailability.AVAILABLE) {
            throw new IllegalArgumentException("AVAILABLE 집계에는 AVAILABLE 카탈로그가 필요합니다.");
        }

        Set<Long> brandIds = new HashSet<>();
        for (BrandRef brand : catalog.brands()) {
            brandIds.add(brand.brandId());
        }
        Map<Long, Long> campaignOwners = new HashMap<>();
        for (CampaignRef campaign : catalog.campaigns()) {
            if (!brandIds.contains(campaign.brandId())) {
                throw new IllegalArgumentException("카탈로그 캠페인의 소유 브랜드가 없습니다.");
            }
            Long previous = campaignOwners.put(campaign.couponId(), campaign.brandId());
            if (previous != null && previous != campaign.brandId()) {
                throw new IllegalArgumentException("카탈로그 캠페인 소속이 중복됩니다: " + campaign.couponId());
            }
        }

        if (monthlyTrend.value() != null) {
            for (DailyIssueAggregate row : monthlyTrend.value()) {
                requireCatalogPair(row.brandId(), row.couponId(), brandIds, campaignOwners);
            }
        }
        if (hourlyHeatmap.value() != null) {
            for (HourlyIssueAggregate row : hourlyHeatmap.value()) {
                requireCatalogPair(row.brandId(), row.couponId(), brandIds, campaignOwners);
            }
        }
        if (issuanceStatuses.value() != null) {
            for (IssuanceStatusAggregate row : issuanceStatuses.value()) {
                requireCatalogPair(row.brandId(), row.couponId(), brandIds, campaignOwners);
            }
        }
    }

    /** 한 집계 행의 브랜드가 존재하고 캠페인의 실제 소유 브랜드와 같은지 확인합니다. */
    private static void requireCatalogPair(
            long brandId,
            long couponId,
            Set<Long> brandIds,
            Map<Long, Long> campaignOwners
    ) {
        Long ownerBrandId = campaignOwners.get(couponId);
        if (!brandIds.contains(brandId) || ownerBrandId == null || ownerBrandId != brandId) {
            throw new IllegalArgumentException(
                    "집계 행이 카탈로그의 브랜드·캠페인 소속과 일치하지 않습니다.");
        }
    }

    /** 분석 데이터가 Mock, 실제 집계 DB 또는 아직 연결되지 않은 원천인지 구분합니다. */
    public enum AnalyticsSourceType {
        MOCK,
        AGGREGATE_DB,
        NONE
    }

    /** Source가 값을 조회할 수 있는지 표현하며 최신성 판정은 포함하지 않습니다. */
    public enum AggregateAvailability {
        AVAILABLE,
        PENDING,
        UNAVAILABLE
    }

    /** 요청 필터의 존재·소속을 판정할 카탈로그 메타데이터입니다. */
    public record CatalogSnapshot(
            AggregateAvailability availability,
            List<BrandRef> brands,
            List<CampaignRef> campaigns
    ) {

        /** AVAILABLE만 메타데이터를 보유하도록 검증하고 목록을 불변 복사합니다. */
        public CatalogSnapshot {
            Objects.requireNonNull(availability, "availability");
            Objects.requireNonNull(brands, "brands");
            Objects.requireNonNull(campaigns, "campaigns");
            brands = List.copyOf(brands);
            campaigns = List.copyOf(campaigns);
            if (availability != AggregateAvailability.AVAILABLE
                    && (!brands.isEmpty() || !campaigns.isEmpty())) {
                throw new IllegalArgumentException("미수집 카탈로그는 메타데이터를 가질 수 없습니다.");
            }
        }

        /** 아직 카탈로그 원천이 연결되지 않은 상태를 만듭니다. */
        public static CatalogSnapshot pending() {
            return new CatalogSnapshot(AggregateAvailability.PENDING, List.of(), List.of());
        }
    }

    /** Source가 조회한 값·가용 상태·실제 집계 완료 시각을 함께 보존합니다. */
    public record AggregateObservation<T>(
            T value,
            AggregateAvailability availability,
            Instant observedAt
    ) {

        /** AVAILABLE과 값 미보유 상태의 값·시각 조합을 검증합니다. */
        public AggregateObservation {
            Objects.requireNonNull(availability, "availability");
            if (availability == AggregateAvailability.AVAILABLE) {
                if (value == null || observedAt == null) {
                    throw new IllegalArgumentException("AVAILABLE 원천에는 value와 observedAt이 필요합니다.");
                }
            } else if (value != null || observedAt != null) {
                throw new IllegalArgumentException(availability + " 원천은 value와 observedAt을 가질 수 없습니다.");
            }
        }

        /** 아직 집계가 생성되지 않은 값 미보유 Observation을 만듭니다. */
        public static <T> AggregateObservation<T> pending() {
            return new AggregateObservation<>(null, AggregateAvailability.PENDING, null);
        }

        /** 원천 조회가 실패한 값 미보유 Observation을 만듭니다. */
        public static <T> AggregateObservation<T> unavailable() {
            return new AggregateObservation<>(null, AggregateAvailability.UNAVAILABLE, null);
        }
    }

    /** 분석 범례와 필터 검증에 사용하는 브랜드 메타데이터입니다. */
    public record BrandRef(long brandId, String brandName) {

        /** 브랜드 식별자와 표시 이름을 검증합니다. */
        public BrandRef {
            requirePositive(brandId, "brandId");
            Objects.requireNonNull(brandName, "brandName");
            if (brandName.isBlank()) {
                throw new IllegalArgumentException("brandName은 비어 있을 수 없습니다.");
            }
        }
    }

    /** 캠페인의 브랜드 소속과 KST 운영 날짜 범위입니다. */
    public record CampaignRef(long couponId, long brandId, LocalDate opensOn, LocalDate closesOn) {

        /** 식별자와 양끝을 포함하는 운영 날짜 범위를 검증합니다. */
        public CampaignRef {
            requirePositive(couponId, "couponId");
            requirePositive(brandId, "brandId");
            Objects.requireNonNull(opensOn, "opensOn");
            Objects.requireNonNull(closesOn, "closesOn");
            if (opensOn.isAfter(closesOn)) {
                throw new IllegalArgumentException("opensOn은 closesOn보다 늦을 수 없습니다.");
            }
        }

        /** 캠페인 운영 기간이 요청 기간과 하루 이상 겹치는지 반환합니다. */
        public boolean overlaps(LocalDate from, LocalDate to) {
            return !closesOn.isBefore(from) && !opensOn.isAfter(to);
        }
    }

    /** 날짜·브랜드·캠페인별 발급 수 집계 행입니다. */
    public record DailyIssueAggregate(
            LocalDate date,
            long brandId,
            long couponId,
            long issueCount
    ) {

        /** 집계 날짜·식별자·수량을 검증합니다. */
        public DailyIssueAggregate {
            Objects.requireNonNull(date, "date");
            requirePositive(brandId, "brandId");
            requirePositive(couponId, "couponId");
            requireNonNegative(issueCount, "issueCount");
        }
    }

    /** KST 날짜·시간·브랜드·캠페인별 발급 수 집계 행입니다. */
    public record HourlyIssueAggregate(
            LocalDate date,
            int hour,
            long brandId,
            long couponId,
            long issueCount
    ) {

        /** 날짜·0~23시·식별자·수량을 검증합니다. */
        public HourlyIssueAggregate {
            Objects.requireNonNull(date, "date");
            if (hour < 0 || hour > 23) {
                throw new IllegalArgumentException("hour는 0부터 23까지여야 합니다.");
            }
            requirePositive(brandId, "brandId");
            requirePositive(couponId, "couponId");
            requireNonNegative(issueCount, "issueCount");
        }
    }

    /** 조회 기간에 발급된 쿠폰의 집계 완료 시점 현재 상태별 수량입니다. */
    public record IssuanceStatusAggregate(
            long brandId,
            long couponId,
            LocalDate windowFrom,
            LocalDate windowTo,
            long totalIssued,
            long currentlyIssued,
            long used,
            long cancelled,
            long expired
    ) {

        /** 기간·수량과 네 상태 합계 보존 불변식을 검증합니다. */
        public IssuanceStatusAggregate {
            requirePositive(brandId, "brandId");
            requirePositive(couponId, "couponId");
            Objects.requireNonNull(windowFrom, "windowFrom");
            Objects.requireNonNull(windowTo, "windowTo");
            if (windowFrom.isAfter(windowTo)) {
                throw new IllegalArgumentException("windowFrom은 windowTo보다 늦을 수 없습니다.");
            }
            requireNonNegative(totalIssued, "totalIssued");
            requireNonNegative(currentlyIssued, "currentlyIssued");
            requireNonNegative(used, "used");
            requireNonNegative(cancelled, "cancelled");
            requireNonNegative(expired, "expired");
            long stateTotal = Math.addExact(
                    Math.addExact(currentlyIssued, used),
                    Math.addExact(cancelled, expired));
            if (stateTotal != totalIssued) {
                throw new IllegalArgumentException("네 현재 상태 합계는 totalIssued와 같아야 합니다.");
            }
        }
    }

    /** 양수 식별자 규칙을 공통 적용합니다. */
    private static void requirePositive(long value, String name) {
        if (value <= 0L) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
    }

    /** 집계 수량의 음수 유입을 차단합니다. */
    private static void requireNonNegative(long value, String name) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + "은 음수일 수 없습니다.");
        }
    }
}

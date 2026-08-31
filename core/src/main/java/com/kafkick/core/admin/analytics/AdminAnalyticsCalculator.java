package com.kafkick.core.admin.analytics;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AggregateObservation;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.BrandRef;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.DailyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.HourlyIssueAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.IssuanceStatusAggregate;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult.BrandTrendPoint;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult.HourlyHeatmapCell;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult.IssuanceStatusDistribution;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult.Observation;
import com.kafkick.core.admin.analytics.AdminAnalyticsResult.StatusCount;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.exception.BusinessException;

/** 기간별 원천 집계 행을 월별 추이·히트맵·현재 상태 분포로 변환합니다. */
public final class AdminAnalyticsCalculator {

    private final AdminAnalyticsFreshnessPolicy freshnessPolicy;

    /** 모든 분석이 공유할 최신성 정책을 주입받습니다. */
    public AdminAnalyticsCalculator(AdminAnalyticsFreshnessPolicy freshnessPolicy) {
        this.freshnessPolicy = Objects.requireNonNull(freshnessPolicy, "freshnessPolicy");
    }

    /** 한 Dataset을 독립적인 세 분석 Observation으로 계산합니다. */
    public AdminAnalyticsResult calculate(
            AdminAnalyticsQuery query,
            AdminAnalyticsDataset dataset,
            Instant evaluatedAt
    ) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(dataset, "dataset");
        Objects.requireNonNull(evaluatedAt, "evaluatedAt");

        List<BrandRef> brands = responseBrands(query, dataset);
        Set<Long> responseBrandIds = brands.stream()
                .map(BrandRef::brandId)
                .collect(Collectors.toUnmodifiableSet());
        return new AdminAnalyticsResult(
                query,
                dataset.sourceType(),
                brands,
                calculateMonthly(
                        query, brands, responseBrandIds, dataset.monthlyTrend(), evaluatedAt),
                calculateHeatmap(query, responseBrandIds, dataset.hourlyHeatmap(), evaluatedAt),
                calculateStatuses(query, responseBrandIds, dataset.issuanceStatuses(), evaluatedAt));
    }

    /** 요청 기간에 운영 쿠폰 회차가 있거나 명시적으로 선택된 브랜드를 응답 모집단으로 고릅니다. */
    private static List<BrandRef> responseBrands(
            AdminAnalyticsQuery query,
            AdminAnalyticsDataset dataset
    ) {
        if (dataset.catalog().availability() != AdminAnalyticsDataset.AggregateAvailability.AVAILABLE) {
            return List.of();
        }
        Map<Long, BrandRef> brands = new LinkedHashMap<>();
        for (BrandRef brand : dataset.catalog().brands()) {
            if (query.brandId() != null && !query.brandId().equals(brand.brandId())) {
                continue;
            }
            boolean selected = query.brandId() != null && query.brandId().equals(brand.brandId());
            boolean hasOverlappingCouponRound = dataset.catalog().couponRounds().stream()
                    .anyMatch(couponRound -> couponRound.brandId() == brand.brandId()
                            && (query.couponId() == null
                            || query.couponId().equals(couponRound.couponId()))
                            && couponRound.overlaps(query.from(), query.to()));
            if (selected || hasOverlappingCouponRound) {
                brands.put(brand.brandId(), brand);
            }
        }
        return List.copyOf(brands.values());
    }

    /** 일별 행을 브랜드·월별로 합산하고 최신 집계만 빈 월을 0으로 채웁니다. */
    private Observation<List<BrandTrendPoint>> calculateMonthly(
            AdminAnalyticsQuery query,
            List<BrandRef> brands,
            Set<Long> responseBrandIds,
            AggregateObservation<List<DailyIssueAggregate>> source,
            Instant evaluatedAt
    ) {
        SourceStatus status = freshness(source, evaluatedAt);
        if (!status.carriesValue()) {
            // 미집계를 정상적인 0건처럼 보이지 않도록 값 미보유 상태에서는 버킷을 만들지 않습니다.
            return new Observation<>(null, status, null);
        }

        Map<BrandMonth, Long> totals = new HashMap<>();
        for (DailyIssueAggregate row : source.value()) {
            if (!inRange(row.date(), query) || !matchesFilter(row.brandId(), row.couponId(), query)) {
                continue;
            }
            if (!responseBrandIds.contains(row.brandId())) {
                // 합계에만 남는 브랜드가 생기지 않도록 카탈로그 기간과 어긋난 원천을 명시적으로 거부합니다.
                throw sourceContractMismatch("월별 집계 행의 브랜드가 응답 모집단에 없습니다.");
            }
            BrandMonth key = new BrandMonth(row.brandId(), YearMonth.from(row.date()));
            totals.merge(key, row.issueCount(), Math::addExact);
        }

        long total = totals.values().stream().reduce(0L, Math::addExact);
        SourceStatus resultStatus = status == SourceStatus.VALID && total == 0L
                ? SourceStatus.NO_TRAFFIC : status;
        List<BrandTrendPoint> points = resultStatus == SourceStatus.STALE
                ? observedMonthlyPoints(totals)
                : continuousMonthlyPoints(query, brands, totals);
        return new Observation<>(points, resultStatus, source.observedAt());
    }

    /** 오래된 집계에는 실제로 관측된 월만 반환해 가짜 0을 만들지 않습니다. */
    private static List<BrandTrendPoint> observedMonthlyPoints(Map<BrandMonth, Long> totals) {
        return totals.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new BrandTrendPoint(
                        entry.getKey().month().atDay(1),
                        entry.getKey().brandId(),
                        entry.getValue()))
                .toList();
    }

    /** 최신 집계에서 요청 범위의 모든 월을 브랜드별로 생성해 빈 구간을 0으로 채웁니다. */
    private static List<BrandTrendPoint> continuousMonthlyPoints(
            AdminAnalyticsQuery query,
            List<BrandRef> brands,
            Map<BrandMonth, Long> totals
    ) {
        List<BrandTrendPoint> points = new ArrayList<>();
        List<BrandRef> orderedBrands = brands.stream()
                .sorted(Comparator.comparingLong(BrandRef::brandId))
                .toList();
        YearMonth first = YearMonth.from(query.from());
        YearMonth last = YearMonth.from(query.to());
        for (BrandRef brand : orderedBrands) {
            for (YearMonth month = first; !month.isAfter(last); month = month.plusMonths(1)) {
                points.add(new BrandTrendPoint(
                        month.atDay(1),
                        brand.brandId(),
                        totals.getOrDefault(new BrandMonth(brand.brandId(), month), 0L)));
            }
        }
        return List.copyOf(points);
    }

    /** 시간 집계 행을 ISO 요일×시간 셀로 합산하고 최신 집계만 168셀을 완성합니다. */
    private Observation<List<HourlyHeatmapCell>> calculateHeatmap(
            AdminAnalyticsQuery query,
            Set<Long> responseBrandIds,
            AggregateObservation<List<HourlyIssueAggregate>> source,
            Instant evaluatedAt
    ) {
        SourceStatus status = freshness(source, evaluatedAt);
        if (!status.carriesValue()) {
            return new Observation<>(null, status, null);
        }

        Map<DayHour, Long> totals = new HashMap<>();
        for (HourlyIssueAggregate row : source.value()) {
            if (!inRange(row.date(), query) || !matchesFilter(row.brandId(), row.couponId(), query)) {
                continue;
            }
            if (!responseBrandIds.contains(row.brandId())) {
                // 월별 추이와 다른 브랜드 모집단이 히트맵에 섞이지 않도록 원천 계약을 검증합니다.
                throw sourceContractMismatch("시간 집계 행의 브랜드가 응답 모집단에 없습니다.");
            }
            DayHour key = new DayHour(row.date().getDayOfWeek().getValue(), row.hour());
            totals.merge(key, row.issueCount(), Math::addExact);
        }
        long total = totals.values().stream().reduce(0L, Math::addExact);
        SourceStatus resultStatus = status == SourceStatus.VALID && total == 0L
                ? SourceStatus.NO_TRAFFIC : status;
        List<HourlyHeatmapCell> cells = resultStatus == SourceStatus.STALE
                ? observedHeatmapCells(totals)
                : completeHeatmapCells(totals);
        return new Observation<>(cells, resultStatus, source.observedAt());
    }

    /** 오래된 히트맵에는 관측된 셀만 안정적인 순서로 반환합니다. */
    private static List<HourlyHeatmapCell> observedHeatmapCells(Map<DayHour, Long> totals) {
        return totals.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> new HourlyHeatmapCell(
                        DayOfWeek.of(entry.getKey().dayOfWeek()),
                        entry.getKey().hour(),
                        entry.getValue()))
                .toList();
    }

    /** 최신 히트맵의 화면 축을 고정하도록 월요일 0시부터 일요일 23시까지 채웁니다. */
    private static List<HourlyHeatmapCell> completeHeatmapCells(Map<DayHour, Long> totals) {
        List<HourlyHeatmapCell> cells = new ArrayList<>(168);
        for (int day = 1; day <= 7; day++) {
            for (int hour = 0; hour <= 23; hour++) {
                cells.add(new HourlyHeatmapCell(
                        DayOfWeek.of(day),
                        hour,
                        totals.getOrDefault(new DayHour(day, hour), 0L)));
            }
        }
        return List.copyOf(cells);
    }

    /** 기간별 현재 상태 수량을 먼저 합산한 뒤 전체 발급 수 기준 비율을 계산합니다. */
    private Observation<IssuanceStatusDistribution> calculateStatuses(
            AdminAnalyticsQuery query,
            Set<Long> responseBrandIds,
            AggregateObservation<List<IssuanceStatusAggregate>> source,
            Instant evaluatedAt
    ) {
        SourceStatus status = freshness(source, evaluatedAt);
        if (!status.carriesValue()) {
            return new Observation<>(null, status, null);
        }

        long total = 0L;
        EnumMap<IssuanceStatus, Long> counts = zeroStatusCounts();
        for (IssuanceStatusAggregate row : source.value()) {
            if (!matchesFilter(row.brandId(), row.couponId(), query)) {
                continue;
            }
            if (!responseBrandIds.contains(row.brandId())) {
                // 응답 브랜드 목록과 상태 분포의 발급 모집단이 갈라지는 원천 행을 거부합니다.
                throw sourceContractMismatch("상태 분포 집계 행의 브랜드가 응답 모집단에 없습니다.");
            }
            if (!row.windowFrom().equals(query.from()) || !row.windowTo().equals(query.to())) {
                // 필터를 통과한 현재 요청 대상에만 동일 기간으로 집계됐다는 계약을 적용합니다.
                throw sourceContractMismatch("상태 분포 집계 기간이 요청 기간과 일치하지 않습니다.");
            }
            total = Math.addExact(total, row.totalIssued());
            merge(counts, IssuanceStatus.ISSUED, row.currentlyIssued());
            merge(counts, IssuanceStatus.USED, row.used());
            merge(counts, IssuanceStatus.CANCELLED, row.cancelled());
            merge(counts, IssuanceStatus.EXPIRED, row.expired());
        }

        // 쿠폰 회차별 비율 평균은 모집단 크기를 잃으므로 수량을 모두 합산한 뒤 한 번만 나눕니다.
        List<StatusCount> statuses = new ArrayList<>(IssuanceStatus.values().length);
        for (IssuanceStatus issuanceStatus : IssuanceStatus.values()) {
            long count = counts.get(issuanceStatus);
            double ratio = total == 0L ? 0D : (double) count / total;
            statuses.add(new StatusCount(issuanceStatus, count, ratio));
        }
        SourceStatus resultStatus = status == SourceStatus.VALID && total == 0L
                ? SourceStatus.NO_TRAFFIC : status;
        return new Observation<>(
                new IssuanceStatusDistribution(total, counts.get(IssuanceStatus.ISSUED), statuses),
                resultStatus,
                source.observedAt());
    }

    /** 네 상태가 항상 같은 순서로 존재하도록 0 수량 맵을 만듭니다. */
    private static EnumMap<IssuanceStatus, Long> zeroStatusCounts() {
        EnumMap<IssuanceStatus, Long> counts = new EnumMap<>(IssuanceStatus.class);
        for (IssuanceStatus status : IssuanceStatus.values()) {
            counts.put(status, 0L);
        }
        return counts;
    }

    /** 상태 수량을 overflow를 숨기지 않고 합산합니다. */
    private static void merge(
            EnumMap<IssuanceStatus, Long> counts,
            IssuanceStatus status,
            long amount
    ) {
        counts.put(status, Math.addExact(counts.get(status), amount));
    }

    /** 한 분석의 가용성과 관측 시각을 공통 기준 시각으로 판정합니다. */
    private SourceStatus freshness(AggregateObservation<?> source, Instant evaluatedAt) {
        return freshnessPolicy.evaluate(source.availability(), source.observedAt(), evaluatedAt);
    }

    /** 날짜가 요청의 양끝 포함 범위에 있는지 확인합니다. */
    private static boolean inRange(LocalDate date, AdminAnalyticsQuery query) {
        return !date.isBefore(query.from()) && !date.isAfter(query.to());
    }

    /** 브랜드·쿠폰 회차 선택 필터를 모두 만족하는 행인지 확인합니다. */
    private static boolean matchesFilter(long brandId, long couponId, AdminAnalyticsQuery query) {
        return (query.brandId() == null || query.brandId() == brandId)
                && (query.couponId() == null || query.couponId() == couponId);
    }

    /** 원천 계약 위반을 외부에 안정적인 분석 오류 코드로 노출합니다. */
    private static BusinessException sourceContractMismatch(String detail) {
        return new BusinessException(AdminAnalyticsErrorCode.SOURCE_CONTRACT_MISMATCH, detail);
    }

    /** 브랜드와 월을 함께 묶는 안정적인 월별 집계 키입니다. */
    private record BrandMonth(long brandId, YearMonth month) implements Comparable<BrandMonth> {

        @Override
        public int compareTo(BrandMonth other) {
            int brandOrder = Long.compare(brandId, other.brandId);
            return brandOrder != 0 ? brandOrder : month.compareTo(other.month);
        }
    }

    /** ISO 요일과 시간을 함께 묶는 안정적인 히트맵 집계 키입니다. */
    private record DayHour(int dayOfWeek, int hour) implements Comparable<DayHour> {

        @Override
        public int compareTo(DayHour other) {
            int dayOrder = Integer.compare(dayOfWeek, other.dayOfWeek);
            return dayOrder != 0 ? dayOrder : Integer.compare(hour, other.hour);
        }
    }
}

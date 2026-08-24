package com.kafkick.core.admin.analytics;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.AnalyticsSourceType;
import com.kafkick.core.admin.analytics.AdminAnalyticsDataset.BrandRef;
import com.kafkick.core.coupon.IssuanceStatus;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 브랜드 분석 계산 결과와 분석별 관측 상태를 제공합니다. */
public record AdminAnalyticsResult(
        AdminAnalyticsQuery query,
        AnalyticsSourceType sourceType,
        List<BrandRef> brands,
        Observation<List<BrandTrendPoint>> brandTrends,
        Observation<List<HourlyHeatmapCell>> hourlyHeatmap,
        Observation<IssuanceStatusDistribution> issuanceStatusDistribution
) {

    /** 결과의 필수 항목과 응답 목록을 불변 복사합니다. */
    public AdminAnalyticsResult {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(sourceType, "sourceType");
        Objects.requireNonNull(brands, "brands");
        Objects.requireNonNull(brandTrends, "brandTrends");
        Objects.requireNonNull(hourlyHeatmap, "hourlyHeatmap");
        Objects.requireNonNull(issuanceStatusDistribution, "issuanceStatusDistribution");
        brands = List.copyOf(brands);
        brandTrends = copyListObservation(brandTrends);
        hourlyHeatmap = copyListObservation(hourlyHeatmap);
    }

    /** 목록 Observation의 값만 불변 복사합니다. */
    private static <T> Observation<List<T>> copyListObservation(Observation<List<T>> observation) {
        if (observation.value() == null) {
            return observation;
        }
        return new Observation<>(
                List.copyOf(observation.value()), observation.status(), observation.observedAt());
    }

    /** 월 시작일 기준 브랜드 발급 합계입니다. */
    public record BrandTrendPoint(LocalDate periodStart, long brandId, long issueCount) { }

    /** 요일과 0~23시 기준 발급 합계이며 숫자 직렬화는 API 경계에서 수행합니다. */
    public record HourlyHeatmapCell(DayOfWeek dayOfWeek, int hour, long issueCount) {

        /** 요일·시간·수량의 기술 중립 Core 계약을 검증합니다. */
        public HourlyHeatmapCell {
            Objects.requireNonNull(dayOfWeek, "dayOfWeek");
            if (hour < 0 || hour > 23) {
                throw new IllegalArgumentException("hour는 0부터 23까지여야 합니다.");
            }
            if (issueCount < 0L) {
                throw new IllegalArgumentException("issueCount는 음수일 수 없습니다.");
            }
        }
    }

    /** 조회 기간 내 전체 발급 수와 집계 시점 현재 상태 분포입니다. */
    public record IssuanceStatusDistribution(
            long totalIssued,
            long currentlyIssued,
            List<StatusCount> statuses
    ) {

        /** 전체 수량과 상태 목록을 불변 복사합니다. */
        public IssuanceStatusDistribution {
            if (totalIssued < 0L || currentlyIssued < 0L) {
                throw new IllegalArgumentException("발급 상태 분포 수량은 음수일 수 없습니다.");
            }
            Objects.requireNonNull(statuses, "statuses");
            statuses = List.copyOf(statuses);
            validateStatusDistribution(totalIssued, currentlyIssued, statuses);
        }

        /** 네 상태가 정확히 한 번씩 존재하고 수량·비율이 전체 발급 수와 일치하는지 검증합니다. */
        private static void validateStatusDistribution(
                long totalIssued,
                long currentlyIssued,
                List<StatusCount> statuses
        ) {
            EnumMap<IssuanceStatus, StatusCount> byStatus = new EnumMap<>(IssuanceStatus.class);
            long stateTotal = 0L;
            for (StatusCount statusCount : statuses) {
                StatusCount previous = byStatus.put(statusCount.status(), statusCount);
                if (previous != null) {
                    throw new IllegalArgumentException("발급 상태 분포에 중복 상태가 있습니다.");
                }
                stateTotal = Math.addExact(stateTotal, statusCount.count());
                double expectedRatio = totalIssued == 0L
                        ? 0D : (double) statusCount.count() / totalIssued;
                if (!equalWithinUlps(statusCount.ratio(), expectedRatio)) {
                    throw new IllegalArgumentException("상태 비율이 totalIssued 기준 계산과 다릅니다.");
                }
            }
            if (byStatus.size() != IssuanceStatus.values().length) {
                throw new IllegalArgumentException("발급 상태 분포에는 네 현재 상태가 모두 필요합니다.");
            }
            if (stateTotal != totalIssued) {
                throw new IllegalArgumentException("현재 상태 합계는 totalIssued와 같아야 합니다.");
            }
            if (byStatus.get(IssuanceStatus.ISSUED).count() != currentlyIssued) {
                throw new IllegalArgumentException("ISSUED 수량은 currentlyIssued와 같아야 합니다.");
            }
        }

        /** JSON 부동소수 직렬화 오차를 허용하면서 계산 비율의 의미가 같은지 비교합니다. */
        private static boolean equalWithinUlps(double actual, double expected) {
            double tolerance = Math.ulp(expected) * 4D;
            return Math.abs(actual - expected) <= tolerance;
        }
    }

    /** 하나의 현재 발급 상태에 해당하는 수량과 전체 대비 비율입니다. */
    public record StatusCount(IssuanceStatus status, long count, double ratio) {

        /** 상태·수량·0~1 비율을 검증합니다. */
        public StatusCount {
            Objects.requireNonNull(status, "status");
            if (count < 0L || !Double.isFinite(ratio) || ratio < 0D || ratio > 1D) {
                throw new IllegalArgumentException("상태 수량 또는 비율이 유효하지 않습니다.");
            }
        }
    }

    /** 계산값과 상태·실제 집계 시각을 함께 보존합니다. */
    public record Observation<T>(T value, SourceStatus status, Instant observedAt) {

        /** 공통 SourceStatus의 값·시각 보유 규칙을 적용합니다. */
        public Observation {
            Objects.requireNonNull(status, "status");
            if (status.carriesValue()) {
                if (value == null || observedAt == null) {
                    throw new IllegalArgumentException(status + " 상태에는 value와 observedAt이 필요합니다.");
                }
            } else if (value != null || observedAt != null) {
                throw new IllegalArgumentException(status + " 상태의 value와 observedAt은 null이어야 합니다.");
            }
        }
    }
}

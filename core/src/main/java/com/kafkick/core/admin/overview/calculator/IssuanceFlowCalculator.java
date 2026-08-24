package com.kafkick.core.admin.overview.calculator;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

/**
 * 실제 관측 구간의 발급 완료 수로 O1 분당 발급률과 추세를 계산하는 순수 계산기입니다.
 *
 * <p>0은 원천이 관측한 실제 완료 0건일 뿐 미수집을 뜻하지 않습니다. 미수집 상태는 수치를 만들지
 * 않고 그대로 전파하며, 이 계산기는 전역 {@code AggregateIssuanceRate}를 만들지 않습니다.</p>
 */
@Component
public class IssuanceFlowCalculator {

    /** 상태 없는 O1 순수 계산기를 생성합니다. */
    public IssuanceFlowCalculator() { }

    /**
     * 캠페인별 실제 경과시간 보정 발급률·버킷 점·상태를 한 번에 계산합니다.
     *
     * <p>{@code currentPerMinute = completedCount / window minutes}이며, 현재 상태의 지속 시간은
     * 마지막 성공·raw scrape 시각이 아니라 연속 조건 시작부터 현재 평가 구간의
     * {@code windowEnd}까지 계산합니다.</p>
     *
     * @param policy 감소율과 중단 시간 정책
     * @param inputs 쿠폰별 완료·시도·구간·원천 상태 입력
     * @return couponId별 O1 관측값; 순서는 입력과 무관하고 Map은 불변
     */
    public IssuanceFlowCalculation calculate(
            OverviewCalculationPolicy policy,
            List<IssuanceFlowInput> inputs
    ) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(inputs, "inputs");
        Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> flows =
                new LinkedHashMap<>();
        for (IssuanceFlowInput input : inputs) {
            Objects.requireNonNull(input, "inputs에는 null을 포함할 수 없습니다.");
            if (flows.put(input.couponId(), calculateOne(policy, input)) != null) {
                throw new IllegalArgumentException("couponId는 중복될 수 없습니다.");
            }
        }
        return new IssuanceFlowCalculation(flows);
    }

    /** 원천에 값이 없으면 O1도 값 없이 보존하고, 그렇지 않으면 실제 시간 기준 수치를 계산합니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> calculateOne(
            OverviewCalculationPolicy policy,
            IssuanceFlowInput input
    ) {
        // 미수집 상태를 0건으로 계산하지 않고 값 없는 관측 결과로 그대로 전달합니다.
        if (!input.sourceStatus().carriesValue()) {
            return new AdminOverviewSnapshot.Observation<>(null, input.sourceStatus(), null);
        }
        requirePositiveWindow(input.windowStart(), input.windowEnd(), "관측 구간");
        // 고정 1분을 가정하지 않고 실제 관측 구간 길이로 분당 발급률을 환산합니다.
        double currentPerMinute = perMinute(input.completedCount(), input.windowStart(), input.windowEnd());
        List<IssuanceBucket> buckets = sortedBuckets(input);
        List<AdminOverviewSnapshot.IssuanceRatePoint> points = buckets.stream()
                .map(bucket -> new AdminOverviewSnapshot.IssuanceRatePoint(
                        bucket.windowEnd(), perMinute(
                                bucket.completedCount(), bucket.windowStart(), bucket.windowEnd())))
                .toList();
        // 시도와 성공의 모집단이 다르므로 두 값이 모두 0일 때만 무트래픽으로 판정합니다.
        boolean noTraffic = input.attemptedCount() == 0d && input.completedCount() == 0d;
        SourceStatus status = noTraffic && input.sourceStatus() == SourceStatus.VALID
                ? SourceStatus.NO_TRAFFIC : input.sourceStatus();
        AdminOverviewSnapshot.IssuanceFlowState state = stateOf(policy, input);
        Duration duration = state == AdminOverviewSnapshot.IssuanceFlowState.NORMAL
                ? null : Duration.between(input.conditionStartedAt(), input.windowEnd());
        return new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.IssuanceFlow(
                currentPerMinute, input.trendWindowStart(), input.trendWindowEnd(), points, state, duration),
                status, input.observedAt());
    }

    /** 수요 없음과 재고 없음은 중단이 아니며, 감소는 비교 구간 대비 정책 비율 이상일 때만 판정합니다. */
    private static AdminOverviewSnapshot.IssuanceFlowState stateOf(
            OverviewCalculationPolicy policy,
            IssuanceFlowInput input
    ) {
        if (input.attemptedCount() == 0d || !input.stockAvailable()) {
            return AdminOverviewSnapshot.IssuanceFlowState.NORMAL;
        }
        Duration duration = Duration.between(input.conditionStartedAt(), input.windowEnd());
        // 수요와 재고가 있는데 성공 0건이 임계시간 이상 지속된 경우에만 중단으로 판정합니다.
        if (input.campaignStatus() == CouponStatus.OPEN && input.completedCount() == 0d
                && !duration.isNegative() && duration.compareTo(policy.issuanceStoppedAfter()) >= 0) {
            return AdminOverviewSnapshot.IssuanceFlowState.STOPPED;
        }
        double currentRate = perMinute(input.completedCount(), input.windowStart(), input.windowEnd());
        double comparisonRate = perMinute(input.comparisonCompletedCount(),
                input.comparisonWindowStart(), input.comparisonWindowEnd());
        // 건수 대신 서로 다른 길이의 두 구간을 각각 rate로 바꾼 뒤 감소율을 비교합니다.
        if (comparisonRate > 0.0 && currentRate <= comparisonRate * (1.0 - policy.issuanceDecreaseRatio())) {
            return AdminOverviewSnapshot.IssuanceFlowState.DECREASING;
        }
        return AdminOverviewSnapshot.IssuanceFlowState.NORMAL;
    }

    /** 완료 수와 실제 구간 초를 분당 단위로 환산합니다. */
    private static double perMinute(double completedCount, Instant start, Instant end) {
        requirePositiveWindow(start, end, "관측 구간");
        Duration duration = Duration.between(start, end);
        double seconds = duration.getSeconds() + (duration.getNano() / 1_000_000_000d);
        double minutes = seconds / 60d;
        // 1분 이상은 먼저 나누어 큰 유한 count의 중간 곱셈 overflow를 피합니다.
        double rate = minutes >= 1d
                ? completedCount / minutes
                : completedCount * (1d / minutes);
        if (!Double.isFinite(rate)) {
            throw new IllegalArgumentException("분당 발급률은 유한해야 합니다.");
        }
        return rate;
    }

    /** 버킷은 추세 구간 안에서 시간순으로 하나씩만 존재해야 그래프가 결정적입니다. */
    private static List<IssuanceBucket> sortedBuckets(IssuanceFlowInput input) {
        List<IssuanceBucket> sorted = input.buckets().stream()
                .sorted(java.util.Comparator.comparing(IssuanceBucket::windowStart))
                .toList();
        Instant previousEnd = null;
        for (IssuanceBucket bucket : sorted) {
            if (bucket.windowStart().isBefore(input.trendWindowStart())
                    || bucket.windowEnd().isAfter(input.trendWindowEnd())
                    || (previousEnd != null && bucket.windowStart().isBefore(previousEnd))) {
                throw new IllegalArgumentException("버킷은 추세 관측 구간 안에서 중첩될 수 없습니다.");
            }
            previousEnd = bucket.windowEnd();
        }
        return sorted;
    }

    /** 0초·역전된 구간을 0 발급률로 위조하지 않도록 거부합니다. */
    private static void requirePositiveWindow(Instant start, Instant end, String name) {
        if (!end.isAfter(start)) {
            throw new IllegalArgumentException(name + "은 양수여야 합니다.");
        }
    }

    /**
     * O1 원천 입력입니다.
     *
     * @param couponId 캠페인 식별자
     * @param campaignStatus 확정 캠페인 상태
     * @param stockAvailable 실제 재고가 남아 발급 중단 판정 대상인지 여부
     * @param windowStart 현재 발급 완료 수를 센 구간 시작 시각
     * @param windowEnd 현재 발급 완료 수를 센 구간 종료 시각
     * @param trendWindowStart 그래프 버킷을 포함하는 추세 구간 시작 시각
     * @param trendWindowEnd 그래프 버킷을 포함하는 추세 구간 종료 시각
     * @param attemptedCount 현재 구간 정책 검증을 통과한 실제 발급 시도 증가량;
     *                       완료 수와 모집단이 달라 둘 다 0일 때만 NO_TRAFFIC 후보
     * @param completedCount 현재 구간 실제 완료 증가량; Prometheus 경계 보간의 소수 값을 보존하며 0은 관측값
     * @param comparisonCompletedCount 비교 구간 실제 완료 증가량; 현재 구간과 같은 소수 정밀도를 보존
     * @param comparisonWindowStart 비교 완료 수를 센 구간 시작 시각
     * @param comparisonWindowEnd 비교 완료 수를 센 구간 종료 시각
     * @param buckets 그래프 버킷별 실제 완료 증가량
     * @param lastCompletedAt 마지막 완료 시각; 양수 completedCount면 {@code [windowStart, windowEnd]}
     *                        폐구간 안에 필수이고, 0이면 해당 구간 안에 있을 수 없음
     * @param conditionStartedAt 연속 무발급 또는 감소 조건 시작 시각
     * @param sourceStatus 원천 관측 상태
     * @param observedAt 원천 실제 관측 시각; 값 없는 상태에서는 null
     */
    public record IssuanceFlowInput(Long couponId, CouponStatus campaignStatus, Boolean stockAvailable,
                                    Instant windowStart, Instant windowEnd,
                                    Instant trendWindowStart, Instant trendWindowEnd,
                                    Double attemptedCount,
                                    Double completedCount, Double comparisonCompletedCount,
                                    Instant comparisonWindowStart, Instant comparisonWindowEnd,
                                    List<IssuanceBucket> buckets, Instant lastCompletedAt,
                                    Instant conditionStartedAt, SourceStatus sourceStatus,
                                    Instant observedAt) {
        /** 입력의 식별자·수량·상태·시간 관계를 검증하고 존재하는 버킷 목록을 불변 복사합니다. */
        public IssuanceFlowInput {
            Objects.requireNonNull(couponId, "couponId");
            Objects.requireNonNull(campaignStatus, "campaignStatus");
            Objects.requireNonNull(sourceStatus, "sourceStatus");
            if (sourceStatus.carriesValue() != (observedAt != null)) {
                throw new IllegalArgumentException("원천 상태와 observedAt 조합이 맞지 않습니다.");
            }
            if (sourceStatus.carriesValue()) {
                Objects.requireNonNull(stockAvailable, "stockAvailable");
                Objects.requireNonNull(windowStart, "windowStart");
                Objects.requireNonNull(windowEnd, "windowEnd");
                Objects.requireNonNull(trendWindowStart, "trendWindowStart");
                Objects.requireNonNull(trendWindowEnd, "trendWindowEnd");
                Objects.requireNonNull(attemptedCount, "attemptedCount");
                Objects.requireNonNull(completedCount, "completedCount");
                Objects.requireNonNull(comparisonCompletedCount, "comparisonCompletedCount");
                Objects.requireNonNull(comparisonWindowStart, "comparisonWindowStart");
                Objects.requireNonNull(comparisonWindowEnd, "comparisonWindowEnd");
                Objects.requireNonNull(buckets, "buckets");
                Objects.requireNonNull(conditionStartedAt, "conditionStartedAt");
                if (!Double.isFinite(attemptedCount) || !Double.isFinite(completedCount)
                        || !Double.isFinite(comparisonCompletedCount)
                        || attemptedCount < 0d || completedCount < 0d || comparisonCompletedCount < 0d) {
                    throw new IllegalArgumentException("발급 count는 유한한 비음수여야 합니다.");
                }
                if (sourceStatus == SourceStatus.NO_TRAFFIC
                        && (attemptedCount != 0d || completedCount != 0d)) {
                    throw new IllegalArgumentException("NO_TRAFFIC 발급 count는 0이어야 합니다.");
                }
                requirePositiveWindow(windowStart, windowEnd, "관측 구간");
                requirePositiveWindow(trendWindowStart, trendWindowEnd, "추세 관측 구간");
                requirePositiveWindow(comparisonWindowStart, comparisonWindowEnd, "비교 관측 구간");
                if (conditionStartedAt.isAfter(windowEnd)) {
                    throw new IllegalArgumentException("conditionStartedAt은 관측 구간 종료 이후일 수 없습니다.");
                }
                if (completedCount > 0d && lastCompletedAt == null) {
                    throw new IllegalArgumentException("완료가 있으면 lastCompletedAt이 필요합니다.");
                }
                if (completedCount > 0d && (lastCompletedAt.isBefore(windowStart)
                        || lastCompletedAt.isAfter(windowEnd))) {
                    throw new IllegalArgumentException("완료가 있으면 lastCompletedAt은 관측 구간 안이어야 합니다.");
                }
                if (completedCount == 0d && lastCompletedAt != null
                        && !lastCompletedAt.isBefore(windowStart) && !lastCompletedAt.isAfter(windowEnd)) {
                    throw new IllegalArgumentException("무완료 구간의 lastCompletedAt은 관측 구간 안에 있을 수 없습니다.");
                }
                if (campaignStatus == CouponStatus.OPEN && stockAvailable && attemptedCount > 0d
                        && completedCount == 0d && lastCompletedAt != null
                        && lastCompletedAt.isAfter(conditionStartedAt)) {
                    throw new IllegalArgumentException("lastCompletedAt은 무발급 conditionStartedAt 이후일 수 없습니다.");
                }
            }
            if (buckets != null) {
                buckets = List.copyOf(buckets);
            }
        }

    }

    /**
     * O1 그래프의 실제 완료 수 버킷입니다.
     *
     * @param windowStart 버킷 시작 시각
     * @param windowEnd 버킷 종료 시각
     * @param completedCount 버킷의 실제 완료 수; 0은 관측된 무완료
     */
    public record IssuanceBucket(Instant windowStart, Instant windowEnd, double completedCount) {
        /** 버킷의 양수 구간과 음수가 아닌 완료 수를 검증합니다. */
        public IssuanceBucket {
            Objects.requireNonNull(windowStart, "windowStart");
            Objects.requireNonNull(windowEnd, "windowEnd");
            requirePositiveWindow(windowStart, windowEnd, "버킷 구간");
            if (!Double.isFinite(completedCount) || completedCount < 0d) {
                throw new IllegalArgumentException("completedCount는 유한한 비음수여야 합니다.");
            }
        }
    }

    /**
     * couponId별 O1 결과입니다.
     *
     * @param issuanceFlows 캠페인별 발급 흐름 관측값; Map은 호출 뒤 변경할 수 없음
     */
    public record IssuanceFlowCalculation(
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows) {
        /** 결과 Map을 불변 복사해 O4가 같은 O1 결과를 안전하게 재사용하게 합니다. */
        public IssuanceFlowCalculation {
            issuanceFlows = Map.copyOf(issuanceFlows);
        }
    }
}

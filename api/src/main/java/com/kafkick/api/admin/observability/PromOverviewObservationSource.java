package com.kafkick.api.admin.observability;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.AggregateIssuanceRate;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.LatencySummary;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.Observation;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeCount;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeInput;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceBucket;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;
import com.kafkick.core.admin.overview.observation.CampaignObservationTarget;
import com.kafkick.core.admin.overview.observation.OverviewObservationData;
import com.kafkick.core.admin.overview.observation.OverviewObservationRequest;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

/** Prometheus의 B 지표 계약을 Core의 기술 중립 Overview 관측 입력으로 변환합니다. */
public class PromOverviewObservationSource implements OverviewObservationSource {

    private static final Logger log = LoggerFactory.getLogger(PromOverviewObservationSource.class);
    private static final Map<String, AdminOverviewSnapshot.CustomerOutcomeType> OUTCOME_TYPES =
            outcomeTypes();

    private final PromTimeQuery instantQuery;
    private final PromRangeQuery rangeQuery;
    private final Duration staleAfter;
    private final Duration totalBudget;
    private final LongSupplier nanoTime;

    /**
     * 시스템 단조 시계를 사용하는 Overview 관측 원천을 생성합니다.
     *
     * @param instantQuery snapshot 평가 시각을 명시하는 instant-vector 경계
     * @param rangeQuery 별도 matrix range 경계
     * @param staleAfter 실제 관측 시각이 오래됐다고 판정할 기간
     * @param totalBudget 새 질의를 시작할 수 있는 전체 기간
     */
    public PromOverviewObservationSource(
            PromTimeQuery instantQuery,
            PromRangeQuery rangeQuery,
            Duration staleAfter,
            Duration totalBudget
    ) {
        this(instantQuery, rangeQuery, staleAfter, totalBudget, System::nanoTime);
    }

    /**
     * 시험 가능한 단조 시계를 명시해 Overview 관측 원천을 생성합니다.
     *
     * @param instantQuery snapshot 평가 시각을 명시하는 instant-vector 경계
     * @param rangeQuery 별도 matrix range 경계
     * @param staleAfter 실제 관측 시각이 오래됐다고 판정할 기간
     * @param totalBudget 새 질의를 시작할 수 있는 전체 기간
     * @param nanoTime 경과시간 측정용 단조 시계
     */
    PromOverviewObservationSource(
            PromTimeQuery instantQuery,
            PromRangeQuery rangeQuery,
            Duration staleAfter,
            Duration totalBudget,
            LongSupplier nanoTime
    ) {
        this.instantQuery = Objects.requireNonNull(instantQuery, "instantQuery");
        this.rangeQuery = Objects.requireNonNull(rangeQuery, "rangeQuery");
        this.staleAfter = requirePositive(staleAfter, "staleAfter");
        this.totalBudget = requirePositive(totalBudget, "totalBudget");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    /** 모든 영역의 실패를 격리해 동일 요청 모집단의 기술 중립 관측 묶음을 반환합니다. */
    @Override
    public OverviewObservationData observe(OverviewObservationRequest request) {
        Objects.requireNonNull(request, "request");
        QueryBudget budget = new QueryBudget(totalBudget, nanoTime);
        // Prometheus range 평가점과 같은 밀리초 경계로 한 번 정규화해 endpoint 조회가 어긋나지 않게 합니다.
        Instant evaluationAt = request.snapshotAt().truncatedTo(ChronoUnit.MILLIS);
        List<IssuanceFlowInput> flowInputs = observeFlows(request, evaluationAt, budget);
        OutcomeInput outcomeInput = observeOutcomes(evaluationAt, budget);
        // 세션 peak 경계가 분리되기 전에는 현재값 옆에 가짜 peak를 놓지 않습니다.
        Observation<AggregateIssuanceRate> aggregateRate =
                new Observation<>(null, SourceStatus.PENDING, null);
        Observation<LatencySummary> latency = observeLatency(evaluationAt, budget);
        return new OverviewObservationData(request, flowInputs, outcomeInput, aggregateRate, latency);
    }

    /** OPEN 캠페인은 grouped 결과로 조립하고 그 밖의 캠페인은 질의 없이 N_A로 만듭니다. */
    private List<IssuanceFlowInput> observeFlows(
            OverviewObservationRequest request, Instant evaluationAt, QueryBudget budget
    ) {
        List<CampaignObservationTarget> openTargets = request.campaignTargets().stream()
                .filter(target -> target.campaignStatus() == CouponStatus.OPEN)
                .toList();
        if (openTargets.isEmpty()) {
            return request.campaignTargets().stream()
                    .map(target -> missingFlow(target, SourceStatus.N_A))
                    .toList();
        }
        Map<Long, IssuanceFlowInput> openInputs;
        try {
            List<PromRangeSeries> trend = rangeQuery(
                    budget, OverviewPrometheusContract.flowTrend(),
                    evaluationAt.minus(OverviewPrometheusContract.TREND_WINDOW), evaluationAt,
                    OverviewPrometheusContract.TREND_STEP);
            List<PromSample> lastSuccess = query(
                    budget, OverviewPrometheusContract.lastSuccessEpoch(), evaluationAt);
            List<PromSample> freshness = query(
                    budget, OverviewPrometheusContract.flowFreshnessEpoch(), evaluationAt);
            List<PromSample> publishFailures = query(
                    budget, OverviewPrometheusContract.attemptPublishFailures(), evaluationAt);
            List<PromSample> publishFailureFreshness = query(
                    budget, OverviewPrometheusContract.attemptPublishFailureFreshnessEpoch(),
                    evaluationAt);
            openInputs = buildOpenFlows(
                    evaluationAt, request.policy(), openTargets, trend,
                    lastSuccess, freshness, publishFailures, publishFailureFreshness);
        } catch (PromQueryException failure) {
            logAreaFailure("O1", failure);
            openInputs = missingFlows(openTargets, SourceStatus.UNAVAILABLE);
        }

        List<IssuanceFlowInput> result = new ArrayList<>();
        for (CampaignObservationTarget target : request.campaignTargets()) {
            result.add(target.campaignStatus() == CouponStatus.OPEN
                    ? openInputs.get(target.couponId())
                    : missingFlow(target, SourceStatus.N_A));
        }
        return List.copyOf(result);
    }

    /** grouped O1 결과를 대상별 독립 count·추세·실제 관측 시각으로 변환합니다. */
    private Map<Long, IssuanceFlowInput> buildOpenFlows(
            Instant snapshotAt,
            OverviewCalculationPolicy policy,
            List<CampaignObservationTarget> targets,
            List<PromRangeSeries> trend,
            List<PromSample> lastSuccess,
            List<PromSample> freshness,
            List<PromSample> publishFailures,
            List<PromSample> publishFailureFreshness
    ) {
        OptionalDouble failureIncrease = singleNonNegativeValue(publishFailures);
        if (failureIncrease.isEmpty()) {
            return missingFlows(targets, SourceStatus.PENDING);
        }
        Optional<Instant> failureObservedAt = minimumEpoch(publishFailureFreshness, snapshotAt);
        if (failureObservedAt.isEmpty()) {
            return missingFlows(targets, SourceStatus.PENDING);
        }
        // 오래된 failure=0은 현재 attempt 모집단의 완전성을 증명하지 못합니다.
        if (statusAt(snapshotAt, failureObservedAt.get()) == SourceStatus.STALE) {
            return missingFlows(targets, SourceStatus.UNAVAILABLE);
        }
        if (failureIncrease.getAsDouble() > 0d) {
            return missingFlows(targets, SourceStatus.UNAVAILABLE);
        }

        Map<Long, Instant> freshnessByCoupon = epochByCoupon(freshness, snapshotAt);
        Map<Long, Instant> lastSuccessByCoupon = epochByCoupon(lastSuccess, snapshotAt);
        Map<StageKey, List<PromRangePoint>> trendByStage = trendByStage(trend);
        Map<Long, IssuanceFlowInput> inputs = new LinkedHashMap<>();
        for (CampaignObservationTarget target : targets) {
            inputs.put(target.couponId(), buildOpenFlow(
                    snapshotAt, policy, target,
                    freshnessByCoupon, lastSuccessByCoupon, trendByStage, failureObservedAt.get()));
        }
        return Map.copyOf(inputs);
    }

    /** 캠페인 하나의 값 존재·신선도·연속 조건을 평가해 O1 입력을 만듭니다. */
    private IssuanceFlowInput buildOpenFlow(
            Instant snapshotAt,
            OverviewCalculationPolicy policy,
            CampaignObservationTarget target,
            Map<Long, Instant> freshnessByCoupon,
            Map<Long, Instant> lastSuccessByCoupon,
            Map<StageKey, List<PromRangePoint>> trendByStage,
            Instant failureObservedAt
    ) {
        Long couponId = target.couponId();
        Instant flowObservedAt = freshnessByCoupon.get(couponId);
        List<PromRangePoint> attemptTrend =
                trendByStage.get(new StageKey(couponId, OverviewPrometheusContract.ATTEMPT));
        List<PromRangePoint> successTrend =
                trendByStage.get(new StageKey(couponId, OverviewPrometheusContract.SUCCESS));
        Instant comparisonEnd = snapshotAt.minus(OverviewPrometheusContract.COMPARISON_OFFSET);
        Double attempts = valueAt(attemptTrend, snapshotAt);
        Double successes = valueAt(successTrend, snapshotAt);
        Double comparisonSuccesses = valueAt(successTrend, comparisonEnd);
        if (attempts == null || successes == null || comparisonSuccesses == null
                || flowObservedAt == null || attemptTrend == null || successTrend == null) {
            return missingFlow(target, SourceStatus.PENDING);
        }
        if (!target.stockStatus().carriesValue()) {
            // 재고 미관측 OPEN은 발급 count가 있어도 STOPPED/NORMAL 값을 추정하지 않습니다.
            return missingFlow(target, target.stockStatus());
        }
        Instant observedAt = flowObservedAt.isBefore(failureObservedAt)
                ? flowObservedAt : failureObservedAt;
        if (target.stockObservedAt().isBefore(observedAt)) {
            observedAt = target.stockObservedAt();
        }

        Instant currentStart = snapshotAt.minus(OverviewPrometheusContract.CURRENT_WINDOW);
        Instant comparisonStart = comparisonEnd.minus(OverviewPrometheusContract.CURRENT_WINDOW);
        Instant trendStart = snapshotAt.minus(OverviewPrometheusContract.TREND_WINDOW);
        ConditionKind conditionKind = conditionKind(
                target.stockAvailable(), attempts, successes, comparisonSuccesses,
                policy.issuanceDecreaseRatio());
        TrendAlignment alignment = alignTrend(
                attemptTrend, successTrend, trendStart, snapshotAt, conditionKind, policy);
        if (alignment.buckets().isEmpty()) {
            return missingFlow(target, SourceStatus.PENDING);
        }
        Instant lastCompletedAt = successes > 0d ? lastSuccessByCoupon.get(couponId) : null;
        if (successes > 0d && (lastCompletedAt == null || lastCompletedAt.isBefore(currentStart)
                || lastCompletedAt.isAfter(snapshotAt))) {
            return missingFlow(target, SourceStatus.PENDING);
        }
        SourceStatus status = statusAt(snapshotAt, observedAt);
        if (status == SourceStatus.VALID && alignment.warmingUpRequired()) {
            status = SourceStatus.WARMING_UP;
        }
        status = combineValueFlowAndStockStatus(
                status, target.stockStatus(), attempts > 0d || successes > 0d);
        return new IssuanceFlowInput(
                couponId, target.campaignStatus(), target.stockAvailable(),
                currentStart, snapshotAt, trendStart, snapshotAt,
                attempts, successes, comparisonSuccesses,
                comparisonStart, comparisonEnd, alignment.buckets(), lastCompletedAt,
                alignment.conditionStartedAt(), status, observedAt);
    }

    /** 값 있는 O1 metric과 재고에서 STALE·WARMING_UP·모순된 NO_TRAFFIC 순으로 보수 상태를 고릅니다. */
    private static SourceStatus combineValueFlowAndStockStatus(
            SourceStatus flowStatus,
            SourceStatus stockStatus,
            boolean flowHasTraffic
    ) {
        if (flowStatus == SourceStatus.STALE || stockStatus == SourceStatus.STALE) {
            return SourceStatus.STALE;
        }
        if (flowStatus == SourceStatus.WARMING_UP || stockStatus == SourceStatus.WARMING_UP) {
            return SourceStatus.WARMING_UP;
        }
        if (stockStatus == SourceStatus.NO_TRAFFIC) {
            // 값 있는 O1 traffic과 재고 NO_TRAFFIC의 모순은 확정 상태 대신 준비 중으로 격리합니다.
            return flowHasTraffic ? SourceStatus.WARMING_UP : SourceStatus.NO_TRAFFIC;
        }
        return flowStatus;
    }

    /** attempt/success range 점을 시각으로 맞추고 성공 그래프와 보수적 조건 시작점을 만듭니다. */
    private static TrendAlignment alignTrend(
            List<PromRangePoint> attemptPoints,
            List<PromRangePoint> successPoints,
            Instant trendStart,
            Instant trendEnd,
            ConditionKind conditionKind,
            OverviewCalculationPolicy policy
    ) {
        Map<Instant, Double> attempts = pointCounts(attemptPoints, trendStart, trendEnd);
        Map<Instant, Double> successes = pointCounts(successPoints, trendStart, trendEnd);
        List<IssuanceBucket> buckets = new ArrayList<>();
        for (int index = OverviewPrometheusContract.expectedTrendBuckets() - 1; index >= 0; index--) {
            Instant bucketEnd = trendEnd.minus(OverviewPrometheusContract.TREND_STEP.multipliedBy(index));
            if (attempts.containsKey(bucketEnd) && successes.containsKey(bucketEnd)) {
                buckets.add(new IssuanceBucket(
                        bucketEnd.minus(OverviewPrometheusContract.TREND_STEP),
                        bucketEnd, successes.get(bucketEnd)));
            }
        }
        Instant conditionStartedAt = trendEnd.minus(OverviewPrometheusContract.CURRENT_WINDOW);
        boolean warmingUpRequired = false;
        if (conditionKind == ConditionKind.STOPPED) {
            boolean historyMissing = false;
            Instant bucketEnd = trendEnd;
            while (bucketEnd.isAfter(trendStart)) {
                Double attempt = attempts.get(bucketEnd);
                Double success = successes.get(bucketEnd);
                if (attempt == null || success == null) {
                    historyMissing = true;
                    break;
                }
                if (attempt == 0d || success != 0d) {
                    break;
                }
                conditionStartedAt = bucketEnd.minus(OverviewPrometheusContract.TREND_STEP);
                bucketEnd = bucketEnd.minus(OverviewPrometheusContract.TREND_STEP);
            }
            Duration provenDuration = Duration.between(conditionStartedAt, trendEnd);
            boolean leftCensored = historyMissing || conditionStartedAt.equals(trendStart);
            warmingUpRequired = leftCensored
                    && provenDuration.compareTo(policy.issuanceStoppedAfter()) < 0;
        } else if (conditionKind == ConditionKind.DECREASING) {
            conditionStartedAt = trendEnd;
            Instant currentEnd = trendEnd;
            while (currentEnd.isAfter(trendStart)) {
                Instant previousEnd = currentEnd.minus(OverviewPrometheusContract.TREND_STEP);
                Double current = successes.get(currentEnd);
                Double previous = successes.get(previousEnd);
                if (current == null || previous == null) {
                    warmingUpRequired = true;
                    break;
                }
                if (previous == 0d
                        || current > previous * (1.0 - policy.issuanceDecreaseRatio())) {
                    break;
                }
                conditionStartedAt = currentEnd.minus(OverviewPrometheusContract.TREND_STEP);
                currentEnd = previousEnd;
            }
            if (conditionStartedAt.equals(trendStart)) {
                warmingUpRequired = true;
            }
        }
        return new TrendAlignment(buckets, conditionStartedAt, warmingUpRequired);
    }

    /** 현재·비교 count로 duration을 추적할 연속 조건 종류를 정합니다. */
    private static ConditionKind conditionKind(
            boolean stockAvailable,
            double attempts,
            double successes,
            double comparisonSuccesses,
            double decreaseRatio
    ) {
        if (!stockAvailable || attempts == 0d) {
            return ConditionKind.NORMAL;
        }
        if (attempts > 0d && successes == 0d) {
            return ConditionKind.STOPPED;
        }
        if (comparisonSuccesses > 0d
                && successes <= comparisonSuccesses * (1.0 - decreaseRatio)) {
            return ConditionKind.DECREASING;
        }
        return ConditionKind.NORMAL;
    }

    /** 최근 5분 O3를 reset-aware Prometheus increase 추정치와 실제 scrape 시각으로 변환합니다. */
    private OutcomeInput observeOutcomes(Instant snapshotAt, QueryBudget budget) {
        try {
            List<PromSample> inventory = query(
                    budget, OverviewPrometheusContract.outcomeInventory(), snapshotAt);
            if (!onlyKnownOutcomeLabels(inventory)) {
                return missingOutcome(SourceStatus.UNAVAILABLE);
            }
            if (completeOutcomeInventory(inventory).isEmpty()) {
                return missingOutcome(SourceStatus.PENDING);
            }
            List<PromSample> samples = query(
                    budget, OverviewPrometheusContract.outcomes(), snapshotAt);
            if (!onlyKnownOutcomeLabels(samples)) {
                return missingOutcome(SourceStatus.UNAVAILABLE);
            }
            Optional<Map<String, Double>> values = completeOutcomeIncreases(samples);
            if (values.isEmpty()) {
                return missingOutcome(SourceStatus.PENDING);
            }
            List<PromSample> freshness = query(
                    budget, OverviewPrometheusContract.outcomeFreshnessEpoch(), snapshotAt);
            Optional<Instant> observedAt = minimumEpoch(freshness, snapshotAt);
            if (observedAt.isEmpty()) {
                return missingOutcome(SourceStatus.PENDING);
            }
            List<OutcomeCount> counts = new ArrayList<>();
            EnumMap<AdminOverviewSnapshot.CustomerOutcomeType, Double> mappedTotals =
                    new EnumMap<>(AdminOverviewSnapshot.CustomerOutcomeType.class);
            double total = 0d;
            for (Map.Entry<String, Double> outcome : values.get().entrySet()) {
                String rawOutcome = outcome.getKey();
                AdminOverviewSnapshot.CustomerOutcomeType type = OUTCOME_TYPES.get(rawOutcome);
                double value = outcome.getValue();
                mappedTotals.merge(type, value, PromOverviewObservationSource::finiteOutcomeSum);
                total = finiteOutcomeSum(total, value);
                counts.add(new OutcomeCount(type, value));
            }
            Instant actualObservedAt = observedAt.get();
            return new OutcomeInput(
                    snapshotAt.minus(OverviewPrometheusContract.OUTCOME_WINDOW), snapshotAt, counts,
                    statusAt(snapshotAt, actualObservedAt), actualObservedAt);
        } catch (PromQueryException failure) {
            logAreaFailure("O3", failure);
            return missingOutcome(SourceStatus.UNAVAILABLE);
        }
    }

    /** O3 벡터가 정의된 raw label만 포함하는지 확인해 새 label을 기존 결과로 오인하지 않습니다. */
    private static boolean onlyKnownOutcomeLabels(List<PromSample> samples) {
        return samples.stream().allMatch(sample ->
                OUTCOME_TYPES.containsKey(sample.label(OverviewPrometheusContract.OUTCOME)));
    }

    /** snapshot inventory가 정확히 13개 known label의 존재를 증명하는지 확인합니다. */
    private static Optional<Map<String, Boolean>> completeOutcomeInventory(List<PromSample> samples) {
        if (samples.size() != OUTCOME_TYPES.size()) {
            return Optional.empty();
        }
        Map<String, Boolean> labels = new LinkedHashMap<>();
        for (PromSample sample : samples) {
            if (!sample.hasNumericValue() || sample.value() <= 0d) {
                throw new PromQueryException("O3 inventory count가 유한한 양수가 아닙니다.");
            }
            String rawOutcome = sample.label(OverviewPrometheusContract.OUTCOME);
            if (labels.put(rawOutcome, Boolean.TRUE) != null) {
                return Optional.empty();
            }
        }
        return labels.keySet().equals(OUTCOME_TYPES.keySet())
                ? Optional.of(Map.copyOf(labels)) : Optional.empty();
    }

    /** 정확히 13개 raw label의 유한한 비음수 increase를 double Map으로 변환합니다. */
    private static Optional<Map<String, Double>> completeOutcomeIncreases(List<PromSample> samples) {
        if (samples.size() != OUTCOME_TYPES.size()) {
            return Optional.empty();
        }
        Map<String, Double> values = new LinkedHashMap<>();
        for (PromSample sample : samples) {
            double value = sample.value();
            if (!Double.isFinite(value) || value < 0d) {
                throw new PromQueryException("O3 increase는 유한한 비음수여야 합니다.");
            }
            String rawOutcome = sample.label(OverviewPrometheusContract.OUTCOME);
            if (values.put(rawOutcome, value) != null) {
                return Optional.empty();
            }
        }
        return values.keySet().equals(OUTCOME_TYPES.keySet())
                ? Optional.of(Map.copyOf(values)) : Optional.empty();
    }

    /** 개별 raw는 유한해도 mapped type 또는 전체 합이 발산하면 O3를 거부합니다. */
    private static double finiteOutcomeSum(double left, double right) {
        double sum = left + right;
        if (!Double.isFinite(sum)) {
            throw new PromQueryException("O3 increase 합계가 유한하지 않습니다.");
        }
        return sum;
    }

    /** 성공 p99의 인스턴스 최댓값과 실제 scrape 시각을 변환합니다. */
    private Observation<LatencySummary> observeLatency(Instant snapshotAt, QueryBudget budget) {
        try {
            List<PromSample> samples = query(
                    budget, OverviewPrometheusContract.successfulP99(), snapshotAt);
            if (samples.isEmpty() || samples.stream()
                    .anyMatch(sample -> !sample.hasNumericValue() || sample.value() <= 0d)) {
                return missingObservation(SourceStatus.PENDING);
            }
            OptionalDouble maximum = samples.stream().mapToDouble(PromSample::value).max();
            List<PromSample> freshness = query(
                    budget, OverviewPrometheusContract.latencyFreshnessEpoch(), snapshotAt);
            Optional<Instant> observedAt = minimumEpoch(freshness, snapshotAt);
            if (observedAt.isEmpty()) {
                return missingObservation(SourceStatus.PENDING);
            }
            Instant actualObservedAt = observedAt.get();
            Duration successfulP99 = latencyDuration(maximum.getAsDouble());
            return new Observation<>(
                    new LatencySummary(successfulP99, null,
                            snapshotAt.minus(OverviewPrometheusContract.LATENCY_WINDOW), snapshotAt),
                    statusAt(snapshotAt, actualObservedAt), actualObservedAt);
        } catch (PromQueryException failure) {
            logAreaFailure("latency", failure);
            return missingObservation(SourceStatus.UNAVAILABLE);
        }
    }

    /** Prometheus 초 단위 지연을 포화 없이 {@link Duration}으로 변환합니다. */
    private static Duration latencyDuration(double seconds) {
        double nanos = seconds * 1_000_000_000d;
        if (!Double.isFinite(nanos) || nanos > Long.MAX_VALUE) {
            throw new PromQueryException("성공 p99가 나노초 변환 범위를 넘습니다: " + seconds);
        }
        return Duration.ofNanos(Math.round(nanos));
    }

    /** 폴링 WARN에는 원인 한 줄만 남기고 stacktrace는 DEBUG에서만 제공합니다. */
    private static void logAreaFailure(String area, RuntimeException failure) {
        log.warn("Overview {} Prometheus 질의 실패로 해당 영역을 UNAVAILABLE로 내려보냅니다: {}",
                area, failure.getMessage());
        log.debug("Overview {} Prometheus 질의 실패 상세", area, failure);
    }

    /** 예산을 확인한 뒤 instant query 하나를 시작합니다. */
    private List<PromSample> query(QueryBudget budget, String promQl, Instant evaluationAt) {
        budget.beforeQuery();
        return instantQuery.query(promQl, evaluationAt);
    }

    /** 예산을 확인한 뒤 range query 하나를 시작합니다. */
    private List<PromRangeSeries> rangeQuery(
            QueryBudget budget, String promQl, Instant start, Instant end, Duration step
    ) {
        budget.beforeQuery();
        return rangeQuery.query(promQl, start, end, step);
    }

    /** 유한한 비음수 단일 표본을 반환하고 빈 결과는 empty로 보존합니다. */
    private static OptionalDouble singleNonNegativeValue(List<PromSample> samples) {
        if (samples.isEmpty()) {
            return OptionalDouble.empty();
        }
        if (samples.size() != 1 || !samples.getFirst().hasNumericValue()
                || samples.getFirst().value() < 0d) {
            throw new PromQueryException("단일 비음수 표본이 아닙니다.");
        }
        return OptionalDouble.of(samples.getFirst().value());
    }

    /** 캠페인별 epoch gauge/timestamp 표본을 실제 시각으로 바꿉니다. */
    private static Map<Long, Instant> epochByCoupon(List<PromSample> samples, Instant snapshotAt) {
        Map<Long, Instant> epochs = new HashMap<>();
        for (PromSample sample : samples) {
            Instant epoch = epochOf(sample.value(), snapshotAt);
            if (epochs.put(couponId(sample), epoch) != null) {
                throw new PromQueryException("grouped 캠페인 시각 표본이 중복되었습니다.");
            }
        }
        return Map.copyOf(epochs);
    }

    /** range 시계열을 캠페인·stage별 점 목록으로 바꿉니다. */
    private static Map<StageKey, List<PromRangePoint>> trendByStage(List<PromRangeSeries> series) {
        Map<StageKey, List<PromRangePoint>> trends = new HashMap<>();
        for (PromRangeSeries item : series) {
            Long couponId = parseCouponId(item.label(OverviewPrometheusContract.COUPON_ID));
            String stage = item.label(OverviewPrometheusContract.STAGE);
            if (!OverviewPrometheusContract.ATTEMPT.equals(stage)
                    && !OverviewPrometheusContract.SUCCESS.equals(stage)) {
                throw new PromQueryException("알 수 없는 range 발급 stage입니다: " + stage);
            }
            if (trends.put(new StageKey(couponId, stage), item.points()) != null) {
                throw new PromQueryException("grouped range 시계열이 중복되었습니다.");
            }
        }
        return Map.copyOf(trends);
    }

    /** range 점을 관측 구간 안의 시각별 비음수 count로 바꿉니다. */
    private static Map<Instant, Double> pointCounts(
            List<PromRangePoint> points, Instant startInclusive, Instant endInclusive
    ) {
        Map<Instant, Double> counts = new HashMap<>();
        for (PromRangePoint point : points) {
            if (point.observedAt().isBefore(startInclusive) || point.observedAt().isAfter(endInclusive)) {
                continue;
            }
            if (counts.put(point.observedAt(), nonNegativeIncrease(point.value())) != null) {
                throw new PromQueryException("range 표본 시각이 중복되었습니다.");
            }
        }
        return Map.copyOf(counts);
    }

    /** 동일 matrix의 지정 평가 시각 값을 current·comparison으로 파생합니다. */
    private static Double valueAt(List<PromRangePoint> points, Instant evaluationAt) {
        if (points == null) {
            return null;
        }
        return pointCounts(points, evaluationAt, evaluationAt).get(evaluationAt);
    }

    /** 여러 freshness 표본 중 가장 오래된 실제 시각을 선택합니다. */
    private static Optional<Instant> minimumEpoch(List<PromSample> samples, Instant snapshotAt) {
        return samples.stream()
                .filter(PromSample::hasNumericValue)
                .map(sample -> epochOf(sample.value(), snapshotAt))
                .min(Comparator.naturalOrder());
    }

    /** coupon_id 라벨을 양수 Long으로 읽습니다. */
    private static Long couponId(PromSample sample) {
        return parseCouponId(sample.label(OverviewPrometheusContract.COUPON_ID));
    }

    /** coupon_id 문자열을 양수 Long으로 읽습니다. */
    private static Long parseCouponId(String raw) {
        try {
            long couponId = Long.parseLong(raw);
            if (couponId <= 0L) {
                throw new NumberFormatException("not positive");
            }
            return couponId;
        } catch (NumberFormatException malformed) {
            throw new PromQueryException("coupon_id를 해석할 수 없습니다: " + raw, malformed);
        }
    }

    /** increase 결과를 음수·비유한·범위 초과 없이 원래 측정값으로 보존합니다. */
    private static double nonNegativeIncrease(double value) {
        if (!Double.isFinite(value) || value < 0d || value >= Long.MAX_VALUE) {
            throw new PromQueryException("유효한 비음수 increase가 아닙니다: " + value);
        }
        return value;
    }

    /** epoch 초를 snapshot 이후가 아닌 실제 관측 시각으로 바꿉니다. */
    private static Instant epochOf(double value, Instant snapshotAt) {
        if (!Double.isFinite(value) || value < 0d || value > 4102444800d) {
            throw new PromQueryException("유효한 epoch 초가 아닙니다: " + value);
        }
        Instant epoch = Instant.ofEpochMilli(Math.round(value * 1000d));
        if (epoch.isAfter(snapshotAt)) {
            throw new PromQueryException("관측 시각이 snapshotAt 이후입니다: " + epoch);
        }
        return epoch;
    }

    /** 실제 관측 시각과 snapshot의 차이로 VALID 또는 STALE을 선택합니다. */
    private SourceStatus statusAt(Instant snapshotAt, Instant observedAt) {
        return Duration.between(observedAt, snapshotAt).compareTo(staleAfter) > 0
                ? SourceStatus.STALE : SourceStatus.VALID;
    }

    /** 대상 목록 전체를 같은 값 없는 상태의 O1 입력으로 만듭니다. */
    private static Map<Long, IssuanceFlowInput> missingFlows(
            List<CampaignObservationTarget> targets, SourceStatus status
    ) {
        Map<Long, IssuanceFlowInput> inputs = new LinkedHashMap<>();
        for (CampaignObservationTarget target : targets) {
            inputs.put(target.couponId(), missingFlow(target, status));
        }
        return Map.copyOf(inputs);
    }

    /** 캠페인 하나를 값 없는 O1 입력으로 만듭니다. */
    private static IssuanceFlowInput missingFlow(CampaignObservationTarget target, SourceStatus status) {
        if (status.carriesValue()) {
            throw new IllegalArgumentException("값 없는 O1 상태가 아닙니다: " + status);
        }
        SourceStatus combinedStatus = combineMissingFlowAndStockStatus(target, status);
        return new IssuanceFlowInput(
                target.couponId(), target.campaignStatus(), null,
                null, null, null, null, null, null, null,
                null, null, List.of(), null, null, combinedStatus, null);
    }

    /** 값 없는 O1 metric과 OPEN 재고 상태 중 UNAVAILABLE, PENDING 순으로 더 나쁜 상태를 선택합니다. */
    private static SourceStatus combineMissingFlowAndStockStatus(
            CampaignObservationTarget target,
            SourceStatus flowStatus
    ) {
        if (target.campaignStatus() != CouponStatus.OPEN || target.stockStatus().carriesValue()) {
            return flowStatus;
        }
        if (flowStatus == SourceStatus.UNAVAILABLE || target.stockStatus() == SourceStatus.UNAVAILABLE) {
            return SourceStatus.UNAVAILABLE;
        }
        return SourceStatus.PENDING;
    }

    /** 값 없는 O3 입력을 만듭니다. */
    private static OutcomeInput missingOutcome(SourceStatus status) {
        if (status.carriesValue()) {
            throw new IllegalArgumentException("값 없는 O3 상태가 아닙니다: " + status);
        }
        return new OutcomeInput(null, null, List.of(), status, null);
    }

    /** 값 없는 Snapshot 관측을 만듭니다. */
    private static <T> Observation<T> missingObservation(SourceStatus status) {
        if (status.carriesValue()) {
            throw new IllegalArgumentException("값 없는 관측 상태가 아닙니다: " + status);
        }
        return new Observation<>(null, status, null);
    }

    /** 양수 Duration 설정만 허용합니다. */
    private static Duration requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + "는 양수여야 합니다.");
        }
        return value;
    }

    /** 13개 raw outcome label을 7개 Core 결과 유형으로 완전 매핑합니다. */
    private static Map<String, AdminOverviewSnapshot.CustomerOutcomeType> outcomeTypes() {
        Map<String, AdminOverviewSnapshot.CustomerOutcomeType> types = new LinkedHashMap<>();
        types.put("ISSUED", AdminOverviewSnapshot.CustomerOutcomeType.ISSUED);
        types.put("QUEUED", AdminOverviewSnapshot.CustomerOutcomeType.QUEUED);
        types.put("QUEUE_REQUIRED", AdminOverviewSnapshot.CustomerOutcomeType.QUEUED);
        types.put("ALREADY_ISSUED", AdminOverviewSnapshot.CustomerOutcomeType.ALREADY_ISSUED);
        types.put("STOCK_EXHAUSTED", AdminOverviewSnapshot.CustomerOutcomeType.STOCK_EXHAUSTED);
        types.put("NOT_OPENED", AdminOverviewSnapshot.CustomerOutcomeType.INELIGIBLE);
        types.put("CAMPAIGN_CLOSED", AdminOverviewSnapshot.CustomerOutcomeType.INELIGIBLE);
        types.put("GRADE_NOT_ELIGIBLE", AdminOverviewSnapshot.CustomerOutcomeType.INELIGIBLE);
        types.put("NO_ENTRY_TOKEN", AdminOverviewSnapshot.CustomerOutcomeType.ENTRY_EXPIRED);
        types.put("ENTRY_TOKEN_EXPIRED", AdminOverviewSnapshot.CustomerOutcomeType.ENTRY_EXPIRED);
        types.put("TEMPORARILY_UNAVAILABLE", AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE);
        types.put("INTERNAL_ERROR", AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE);
        types.put("UNMAPPED", AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE);
        return Map.copyOf(types);
    }

    /** 캠페인과 stage의 grouped 결과 키입니다. */
    private record StageKey(Long couponId, String stage) {
        /** 키 구성요소를 필수로 검증합니다. */
        private StageKey {
            Objects.requireNonNull(couponId, "couponId");
            Objects.requireNonNull(stage, "stage");
        }
    }

    /** 정렬된 성공 버킷과 연속 조건 시작·history 충분 여부입니다. */
    private record TrendAlignment(
            List<IssuanceBucket> buckets,
            Instant conditionStartedAt,
            boolean warmingUpRequired
    ) {
        /** 목록을 불변 복사하고 필수 시각을 검증합니다. */
        private TrendAlignment {
            buckets = List.copyOf(Objects.requireNonNull(buckets, "buckets"));
            Objects.requireNonNull(conditionStartedAt, "conditionStartedAt");
        }
    }

    /** range suffix에서 연속 시작 시각을 추적할 현재 O1 조건입니다. */
    private enum ConditionKind {
        NORMAL,
        STOPPED,
        DECREASING
    }

    /** 응답 전체에서 새 Prometheus 질의를 시작할 수 있는 시간 예산입니다. */
    private static final class QueryBudget {
        private final long startedAt;
        private final long budgetNanos;
        private final LongSupplier nanoTime;
        private int startedQueries;

        /** 전체 예산과 단조 시계를 보존합니다. */
        private QueryBudget(Duration budget, LongSupplier nanoTime) {
            this.nanoTime = nanoTime;
            this.startedAt = nanoTime.getAsLong();
            this.budgetNanos = budget.toNanos();
        }

        /** 첫 질의는 허용하고 이후에는 예산 만료 시 새 질의를 거부합니다. */
        private void beforeQuery() {
            long elapsed = nanoTime.getAsLong() - startedAt;
            if (startedQueries > 0 && elapsed >= budgetNanos) {
                throw new PromQueryException("Overview Prometheus 질의 시작 예산이 만료되었습니다.");
            }
            startedQueries++;
        }
    }
}

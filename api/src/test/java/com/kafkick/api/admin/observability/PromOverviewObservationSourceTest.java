package com.kafkick.api.admin.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.hamcrest.Matchers;
import org.springframework.http.MediaType;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeCount;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;
import com.kafkick.core.admin.overview.observation.CampaignObservationTarget;
import com.kafkick.core.admin.overview.observation.OverviewObservationData;
import com.kafkick.core.admin.overview.observation.OverviewObservationRequest;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** Overview용 Prometheus adapter의 grouped 조회와 기술 중립 변환을 검증합니다. */
@ExtendWith(OutputCaptureExtension.class)
class PromOverviewObservationSourceTest {

    private static final Instant SNAPSHOT = Instant.parse("2026-08-23T03:00:00Z");
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.5, Duration.ofMinutes(2), Duration.ofMinutes(3),
            Duration.ofMinutes(2), Duration.ofMinutes(5));

    /** 미래 재고 시각이 더 이른 Prom provenance의 min 처리로 숨겨지는 회귀를 요청 경계에서 막습니다. */
    @Test
    @DisplayName("미래 stock 시각과 과거 metric 조합은 Prom 관측 전에 거부한다")
    void rejectsFutureStockBeforePromMinimumCanHideItsProvenance() {
        PromOverviewObservationSource source = new PromOverviewObservationSource(
                new RecordingPromQuery(query -> {
                    List<PromSample> samples = happyInstant(query);
                    if (!query.contains("timestamp(")) {
                        return samples;
                    }
                    return samples.stream()
                            .map(sample -> new PromSample(
                                    sample.metricName(), sample.labels(),
                                    SNAPSHOT.minusSeconds(1L).getEpochSecond(), sample.evaluatedAt()))
                            .toList();
                }),
                new RecordingRangeQuery(this::happyRange),
                Duration.ofMinutes(2), Duration.ofSeconds(10));

        assertThatThrownBy(() -> source.observe(new OverviewObservationRequest(
                SNAPSHOT,
                List.of(new CampaignObservationTarget(
                        101L, CouponRoundStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT.plusNanos(1L))),
                POLICY)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stockObservedAt");
    }

    /** Overview의 모든 instant 값과 freshness는 한 snapshot 평가 시각을 공유해야 합니다. */
    @Test
    @DisplayName("모든 instant query를 request snapshotAt에 고정한다")
    void anchorsEveryInstantQueryToRequestSnapshot() {
        RecordingPromQuery instant = new RecordingPromQuery(this::happyInstant);
        PromOverviewObservationSource source = new PromOverviewObservationSource(
                instant, new RecordingRangeQuery(this::happyRange),
                Duration.ofMinutes(2), Duration.ofSeconds(10));

        source.observe(request());

        assertThat(instant.unanchoredQueries).isEmpty();
        assertThat(instant.evaluationTimes).containsOnly(SNAPSHOT);
    }

    /** OPEN 재고 미관측은 수치로 추정하지 않고 해당 캠페인의 O1 상태로 그대로 보존합니다. */
    @Test
    @DisplayName("OPEN 재고 PENDING UNAVAILABLE을 캠페인 O1 값 없는 상태로 보존한다")
    void preservesMissingOpenStockStatusAsMissingFlow() {
        for (SourceStatus stockStatus : List.of(SourceStatus.PENDING, SourceStatus.UNAVAILABLE)) {
            OverviewObservationRequest request = new OverviewObservationRequest(SNAPSHOT, List.of(
                    new CampaignObservationTarget(101L, CouponRoundStatus.OPEN, null, stockStatus)), POLICY);

            OverviewObservationData data = new PromOverviewObservationSource(
                    new RecordingPromQuery(this::happyInstant),
                    new RecordingRangeQuery(this::happyRange),
                    Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request);

            assertThat(input(data, 101L).sourceStatus()).isEqualTo(stockStatus);
            assertThat(input(data, 101L).stockAvailable()).isNull();
            assertThat(input(data, 101L).observedAt()).isNull();
            assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.VALID);
            assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.VALID);
        }
    }

    /** O1 metric과 재고 중 값 없는 상태가 겹치면 UNAVAILABLE을 PENDING보다 우선합니다. */
    @Test
    @DisplayName("OPEN 재고와 O1 metric 상태 중 더 나쁜 값 없는 상태를 보존한다")
    void preservesWorseStatusBetweenStockAndFlowMetric() {
        OverviewObservationRequest pendingStockRequest = new OverviewObservationRequest(SNAPSHOT, List.of(
                new CampaignObservationTarget(101L, CouponRoundStatus.OPEN, null, SourceStatus.PENDING)), POLICY);
        OverviewObservationRequest unavailableStockRequest = new OverviewObservationRequest(SNAPSHOT, List.of(
                new CampaignObservationTarget(101L, CouponRoundStatus.OPEN, null, SourceStatus.UNAVAILABLE)), POLICY);

        OverviewObservationData metricUnavailable = new PromOverviewObservationSource(
                new RecordingPromQuery(this::happyInstant),
                new RecordingRangeQuery(query -> {
                    throw new PromQueryException("range failure");
                }), Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(pendingStockRequest);
        OverviewObservationData metricPending = observe(
                query -> query.equals(OverviewPrometheusContract.flowFreshnessEpoch())
                        ? List.of() : happyInstant(query),
                this::happyRange,
                unavailableStockRequest);

        assertThat(input(metricUnavailable, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(input(metricPending, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 값 있는 재고 최신성이 fresh STOPPED metric에 의해 VALID로 승격되는 회귀를 막습니다. */
    @Test
    @DisplayName("값 있는 재고 상태와 fresh O1 metric을 보수적으로 합성한다")
    void combinesValueCarryingStockStatusConservativelyWithFreshStoppedMetric() {
        for (Map.Entry<SourceStatus, SourceStatus> expectation : Map.of(
                SourceStatus.VALID, SourceStatus.VALID,
                SourceStatus.STALE, SourceStatus.STALE,
                SourceStatus.WARMING_UP, SourceStatus.WARMING_UP,
                SourceStatus.NO_TRAFFIC, SourceStatus.WARMING_UP).entrySet()) {
            Instant stockObservedAt = expectation.getKey() == SourceStatus.STALE
                    ? SNAPSHOT.minus(Duration.ofMinutes(3))
                    : expectation.getKey() == SourceStatus.VALID
                    ? SNAPSHOT : SNAPSHOT.minus(Duration.ofMinutes(1));
            OverviewObservationRequest request = new OverviewObservationRequest(SNAPSHOT, List.of(
                    new CampaignObservationTarget(
                            101L, CouponRoundStatus.OPEN, true, expectation.getKey(), stockObservedAt)), POLICY);
            OverviewObservationData data = new PromOverviewObservationSource(
                    new RecordingPromQuery(this::happyInstant),
                    new RecordingRangeQuery(query -> List.of(
                            new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                    gridPoints(1d)),
                            new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                    gridPoints(0d)))),
                    Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request);

            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> calculated =
                    new IssuanceFlowCalculator().calculate(POLICY, List.of(input(data, 101L)))
                            .issuanceFlows().get(101L);

            assertThat(input(data, 101L).sourceStatus()).as("stock=%s", expectation.getKey())
                    .isEqualTo(expectation.getValue());
            assertThat(input(data, 101L).observedAt()).as("stock=%s", expectation.getKey())
                    .isEqualTo(stockObservedAt);
            assertThat(calculated.value().state()).isEqualTo(
                    AdminOverviewSnapshot.IssuanceFlowState.STOPPED);
            if (expectation.getValue() != SourceStatus.VALID) {
                assertThat(new com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator()
                        .calculate(Map.of(101L, calculated))).isEmpty();
            }
        }
    }

    /** 캠페인 수와 무관한 grouped 조회로 1분 현재값·10분 추세·O3·성공 p99를 조립합니다. */
    @Test
    @DisplayName("grouped query 결과를 모든 Overview 관측 입력으로 변환한다")
    void convertsGroupedQueriesToOverviewInputs() {
        RecordingPromQuery instant = new RecordingPromQuery(this::happyInstant);
        RecordingRangeQuery range = new RecordingRangeQuery(this::happyRange);
        PromOverviewObservationSource source = new PromOverviewObservationSource(
                instant, range, Duration.ofMinutes(2), Duration.ofSeconds(10));
        OverviewObservationRequest request = new OverviewObservationRequest(SNAPSHOT, List.of(
                new CampaignObservationTarget(
                        101L, CouponRoundStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT),
                new CampaignObservationTarget(
                        102L, CouponRoundStatus.OPEN, false, SourceStatus.VALID, SNAPSHOT),
                new CampaignObservationTarget(
                        103L, CouponRoundStatus.CLOSED, null, SourceStatus.N_A, null)), POLICY);

        OverviewObservationData data = source.observe(request);

        assertThat(data.issuanceFlowInputs()).hasSize(3);
        IssuanceFlowInput first = input(data, 101L);
        assertThat(first.sourceStatus()).as(instant.queries.toString()).isEqualTo(SourceStatus.VALID);
        assertThat(first.attemptedCount()).isEqualTo(2d);
        assertThat(first.completedCount()).isEqualTo(5d);
        assertThat(first.comparisonCompletedCount()).isEqualTo(8d);
        assertThat(Duration.between(first.windowStart(), first.windowEnd())).isEqualTo(Duration.ofMinutes(1));
        assertThat(Duration.between(first.trendWindowStart(), first.trendWindowEnd()))
                .isEqualTo(Duration.ofMinutes(10));
        assertThat(first.buckets()).hasSize(10);
        assertThat(first.conditionStartedAt()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(1)));
        assertThat(input(data, 103L).sourceStatus()).isEqualTo(SourceStatus.N_A);

        assertThat(instant.queries).doesNotContain(
                OverviewPrometheusContract.currentFlow(),
                OverviewPrometheusContract.comparisonSuccess());
        assertThat(range.queries).singleElement()
                .satisfies(query -> assertThat(query).contains("sum by (coupon_id, stage)"));
        assertThat(instant.queries).noneMatch(query -> query.contains("coupon_id=\"101\"")
                || query.contains("coupon_id=\"102\""));
        assertThat(instant.calls).contains(new InstantCall(
                "count by (outcome) (app_issuance_outcome_total)", SNAPSHOT));

        EnumMap<AdminOverviewSnapshot.CustomerOutcomeType, Double> counts = new EnumMap<>(
                AdminOverviewSnapshot.CustomerOutcomeType.class);
        for (OutcomeCount count : data.outcomeInput().counts()) {
            counts.merge(count.type(), count.count(), Double::sum);
        }
        assertThat(counts).containsExactlyInAnyOrderEntriesOf(Map.of(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 1d,
                AdminOverviewSnapshot.CustomerOutcomeType.QUEUED, 5d,
                AdminOverviewSnapshot.CustomerOutcomeType.ALREADY_ISSUED, 4d,
                AdminOverviewSnapshot.CustomerOutcomeType.STOCK_EXHAUSTED, 5d,
                AdminOverviewSnapshot.CustomerOutcomeType.INELIGIBLE, 35d,
                AdminOverviewSnapshot.CustomerOutcomeType.ENTRY_EXPIRED, 19d,
                AdminOverviewSnapshot.CustomerOutcomeType.SYSTEM_FAILURE, 36d));
        assertThat(data.aggregateIssuanceRate().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.VALID);
        assertThat(data.latencySummary().value().successfulP99()).isEqualTo(Duration.ofMillis(400));
        assertThat(data.latencySummary().value().failedP99()).isNull();
    }

    /** 같은 평가 시각이어도 별도 HTTP 질의 사이에 뒤늦은 scrape가 들어오면 모집단이 달라집니다. */
    @Test
    @DisplayName("O1 current comparison과 trend는 하나의 range matrix endpoint에서만 파생한다")
    void derivesCurrentAndComparisonOnlyFromRangeEndpoints() {
        RecordingPromQuery instant = new RecordingPromQuery(query -> {
            if (query.equals(OverviewPrometheusContract.currentFlow())) {
                return List.of(
                        sample(Map.of("coupon_id", "101", "stage", "attempt"), 999d),
                        sample(Map.of("coupon_id", "101", "stage", "success"), 0d));
            }
            if (query.equals(OverviewPrometheusContract.comparisonSuccess())) {
                return List.of(sample(Map.of("coupon_id", "101"), 777d));
            }
            return happyInstant(query);
        });
        RecordingRangeQuery range = new RecordingRangeQuery(query -> List.of(
                new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                        gridPoints(3d)),
                new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                        gridPoints(List.of(9d, 9d, 9d, 9d, 9d, 9d, 9d, 9d, 9d, 4d, 2d)))));

        IssuanceFlowInput flow = input(new PromOverviewObservationSource(
                instant, range, Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request()), 101L);

        assertThat(flow.attemptedCount()).isEqualTo(3d);
        assertThat(flow.completedCount()).isEqualTo(2d);
        assertThat(flow.comparisonCompletedCount()).isEqualTo(4d);
        assertThat(instant.queries).doesNotContain(
                OverviewPrometheusContract.currentFlow(),
                OverviewPrometheusContract.comparisonSuccess());
        assertThat(range.calls).containsExactly(new RangeCall(
                SNAPSHOT.minus(Duration.ofMinutes(10)), SNAPSHOT, Duration.ofMinutes(1)));
    }

    /** Prometheus range 평가 경계는 밀리초이므로 나노초 요청도 같은 endpoint 표본을 찾아야 합니다. */
    @Test
    @DisplayName("나노초 snapshot도 밀리초 range endpoint의 현재값과 일치한다")
    void matchesNanosecondSnapshotToMillisecondRangeEndpoint() {
        Instant nanosecondSnapshot = SNAPSHOT.plusNanos(456_789L);
        OverviewObservationRequest request = new OverviewObservationRequest(
                nanosecondSnapshot,
                List.of(new CampaignObservationTarget(
                        101L, CouponRoundStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT)),
                POLICY);

        OverviewObservationData data = observe(this::happyInstant, this::happyRange, request);

        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(input(data, 101L).attemptedCount()).isEqualTo(2d);
        assertThat(input(data, 101L).completedCount()).isEqualTo(5d);
    }

    /** scrape freshness가 step과 어긋나도 snapshot grid의 10개 버킷이 Core 입력으로 유효해야 합니다. */
    @Test
    @DisplayName("비정렬 scrape 시각에서도 snapshot 기준 10분 추세가 Core 계산기를 통과한다")
    void keepsSnapshotGridWhenScrapeFreshnessIsUnaligned() {
        Instant scrapeAt = SNAPSHOT.minusSeconds(17);
        RecordingRangeQuery range = new RecordingRangeQuery(query -> List.of(
                new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                        gridPoints(2d)),
                new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                        gridPoints(1d))));
        RecordingPromQuery instant = new RecordingPromQuery(
                query -> {
                    if (query.equals(OverviewPrometheusContract.flowFreshnessEpoch())) {
                        return List.of(sample(Map.of("coupon_id", "101"), scrapeAt.getEpochSecond()));
                    }
                    if (query.equals(OverviewPrometheusContract.outcomeFreshnessEpoch())
                            || query.equals(OverviewPrometheusContract.latencyFreshnessEpoch())) {
                        return List.of(sample(Map.of(), scrapeAt.getEpochSecond()));
                    }
                    return happyInstant(query);
                });
        OverviewObservationData data = new PromOverviewObservationSource(
                instant, range, Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request());

        IssuanceFlowInput flow = input(data, 101L);
        assertThat(flow.sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(flow.observedAt()).isEqualTo(scrapeAt);
        assertThat(flow.windowStart()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(1)));
        assertThat(flow.windowEnd()).isEqualTo(SNAPSHOT);
        assertThat(flow.comparisonWindowStart()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(2)));
        assertThat(flow.comparisonWindowEnd()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(1)));
        assertThat(flow.trendWindowStart()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(10)));
        assertThat(flow.trendWindowEnd()).isEqualTo(SNAPSHOT);
        assertThat(flow.buckets()).hasSize(10);
        assertThat(flow.buckets().getFirst().windowStart())
                .isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(10)));
        assertThat(flow.buckets().getLast().windowEnd()).isEqualTo(SNAPSHOT);
        assertThat(range.calls).containsExactly(new RangeCall(
                SNAPSHOT.minus(Duration.ofMinutes(10)), SNAPSHOT, Duration.ofMinutes(1)));
        assertThat(new IssuanceFlowCalculator().calculate(POLICY, List.of(flow))
                .issuanceFlows().get(101L).value().windowEnd()).isEqualTo(SNAPSHOT);
        assertThat(data.outcomeInput().windowStart()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(5)));
        assertThat(data.outcomeInput().windowEnd()).isEqualTo(SNAPSHOT);
        assertThat(data.outcomeInput().observedAt()).isEqualTo(scrapeAt);
        assertThat(data.latencySummary().value().windowStart()).isEqualTo(SNAPSHOT.minusSeconds(10));
        assertThat(data.latencySummary().value().windowEnd()).isEqualTo(SNAPSHOT);
        assertThat(data.latencySummary().observedAt()).isEqualTo(scrapeAt);
    }

    /** 빈 시계열은 PENDING이고 명시적 0은 계산 가능한 실제 0으로 보존합니다. */
    @Test
    @DisplayName("빈 발급 시계열과 명시적 0을 구분한다")
    void distinguishesEmptyFlowFromMeasuredZero() {
        OverviewObservationData empty = observe(
                this::happyInstant,
                query -> List.of());
        OverviewObservationData zero = observe(
                this::happyInstant,
                query -> List.of(
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                gridPoints(0d)),
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                gridPoints(0d))));

        assertThat(input(empty, 101L).sourceStatus()).isEqualTo(SourceStatus.PENDING);
        assertThat(input(zero, 101L).sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(input(zero, 101L).attemptedCount()).isZero();
        assertThat(input(zero, 101L).completedCount()).isZero();
    }

    /** 숫자 count가 있어도 실제 scrape 시각을 얻지 못하면 현재 관측으로 위장하지 않습니다. */
    @Test
    @DisplayName("발급 count의 실제 관측 시각을 모르면 PENDING이다")
    void keepsNumericFlowPendingWhenFreshnessIsUnknown() {
        OverviewObservationData data = observe(
                query -> query.equals(OverviewPrometheusContract.flowFreshnessEpoch())
                        ? List.of() : happyInstant(query),
                this::happyRange);

        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.PENDING);
        assertThat(input(data, 101L).observedAt()).isNull();
    }

    /** 연속 상태의 전체 10분 이력이 없으면 시작 시각을 더 오래됐다고 추측하지 않습니다. */
    @Test
    @DisplayName("연속 조건 history가 부족하면 값 있는 WARMING_UP이다")
    void marksIncompleteConditionHistoryAsWarmingUp() {
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> List.of(
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                List.of(new PromRangePoint(SNAPSHOT, 1d))),
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                endpointPoints(0d, 0d))));

        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.WARMING_UP);
        assertThat(input(data, 101L).conditionStartedAt())
                .isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(1)));
    }

    /** 10분 전체가 수요 있음·성공 0이면 더 오래됐다고 추측하지 않고 보유 시작점에서 STOPPED입니다. */
    @Test
    @DisplayName("증명된 10분 연속 무발급의 시작과 STOPPED duration을 보존한다")
    void derivesContinuousStoppedConditionThroughCalculator() {
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> List.of(
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                gridPoints(1d)),
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                gridPoints(0d))));

        IssuanceFlowInput flow = input(data, 101L);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> calculated =
                new IssuanceFlowCalculator().calculate(POLICY, List.of(flow)).issuanceFlows().get(101L);

        assertThat(flow.sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(flow.conditionStartedAt()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(10)));
        assertThat(calculated.value().state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.STOPPED);
        assertThat(calculated.value().stateDuration()).isEqualTo(Duration.ofMinutes(10));
    }

    /** 감소 비율을 만족하는 최신 suffix를 뒤로 추적해 Core가 같은 duration을 계산해야 합니다. */
    @Test
    @DisplayName("연속 감소 suffix의 보수적 시작과 DECREASING duration을 보존한다")
    void derivesContinuousDecreasingConditionThroughCalculator() {
        List<Double> decreasing = List.of(
                100d, 100d, 100d, 100d, 100d, 100d, 16d, 8d, 4d, 2d, 1d);
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> List.of(
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                gridPoints(2d)),
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                gridPoints(decreasing))));

        IssuanceFlowInput flow = input(data, 101L);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> calculated =
                new IssuanceFlowCalculator().calculate(POLICY, List.of(flow)).issuanceFlows().get(101L);

        assertThat(flow.conditionStartedAt()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(5)));
        assertThat(calculated.value().state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.DECREASING);
        assertThat(calculated.value().stateDuration()).isEqualTo(Duration.ofMinutes(5));
    }

    /** 최신 감소 pair를 range로 증명할 수 없으면 임의의 1분 시작을 만들 수 없습니다. */
    @Test
    @DisplayName("range-only O1의 최신 또는 직전 endpoint가 없으면 PENDING이다")
    void keepsFlowPendingWhenCurrentOrComparisonRangeEndpointIsMissing() {
        List<Double> decreasing = List.of(
                100d, 100d, 100d, 100d, 100d, 100d, 16d, 8d, 4d, 2d, 1d);
        for (Instant missingAt : List.of(SNAPSHOT, SNAPSHOT.minus(Duration.ofMinutes(1)))) {
            OverviewObservationData data = observe(
                    this::happyInstant,
                    query -> List.of(
                            new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                    gridPoints(2d)),
                            new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                    withoutPoint(gridPoints(decreasing), missingAt))));

            IssuanceFlowInput flow = input(data, 101L);
            assertThat(flow.sourceStatus()).as("missing " + missingAt)
                    .isEqualTo(SourceStatus.PENDING);
            assertThat(flow.conditionStartedAt()).as("missing " + missingAt).isNull();
        }
    }

    /** 보유한 모든 pair가 감소하면 실제 시작은 더 과거일 수 있으므로 경계 시작을 확정할 수 없습니다. */
    @Test
    @DisplayName("DECREASING suffix가 보유 history 왼쪽 경계에 닿으면 WARMING_UP이다")
    void marksLeftCensoredDecreasingSuffixAsWarmingUp() {
        List<Double> leftCensored = List.of(
                1024d, 512d, 256d, 128d, 64d, 32d, 16d, 8d, 4d, 2d, 1d);
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> List.of(
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                gridPoints(2d)),
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                gridPoints(leftCensored))));

        IssuanceFlowInput flow = input(data, 101L);
        assertThat(flow.sourceStatus()).isEqualTo(SourceStatus.WARMING_UP);
        assertThat(flow.conditionStartedAt()).isEqualTo(SNAPSHOT.minus(Duration.ofMinutes(10)));
    }

    /** history가 짧아도 현재 성공이 양수이고 감소 임계 미만이면 duration 판정이 필요하지 않습니다. */
    @Test
    @DisplayName("불완전 trend의 정상 positive-success 관측은 VALID이다")
    void keepsIncompleteNormalPositiveSuccessValid() {
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> List.of(
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                endpointPoints(2d, 2d)),
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                endpointPoints(5d, 5d))));

        IssuanceFlowInput flow = input(data, 101L);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> calculated =
                new IssuanceFlowCalculator().calculate(POLICY, List.of(flow)).issuanceFlows().get(101L);

        assertThat(flow.sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(calculated.value().state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** 재고가 없으면 Core가 NORMAL로 확정하므로 zero-success duration history가 필요하지 않습니다. */
    @Test
    @DisplayName("재고 없는 캠페인의 불완전 무발급 history는 WARMING_UP이 아니다")
    void keepsIncompleteZeroSuccessValidWhenStockIsUnavailable() {
        RecordingPromQuery instant = new RecordingPromQuery(this::happyInstant);
        RecordingRangeQuery range = new RecordingRangeQuery(query -> List.of(
                new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                        endpointPoints(1d, 1d)),
                new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                        endpointPoints(0d, 0d))));
        OverviewObservationRequest request = new OverviewObservationRequest(SNAPSHOT, List.of(
                new CampaignObservationTarget(
                        101L, CouponRoundStatus.OPEN, false, SourceStatus.VALID, SNAPSHOT)), POLICY);

        IssuanceFlowInput flow = input(new PromOverviewObservationSource(
                instant, range, Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request), 101L);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> calculated =
                new IssuanceFlowCalculator().calculate(POLICY, List.of(flow)).issuanceFlows().get(101L);

        assertThat(flow.sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(calculated.value().state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** 실제 scrape 시각이 임계보다 오래되면 값과 시각을 버리지 않고 STALE로 보존합니다. */
    @Test
    @DisplayName("오래된 실제 관측 시각은 값을 가진 STALE이다")
    void preservesValuesAndActualTimeWhenStale() {
        Instant staleAt = SNAPSHOT.minus(Duration.ofMinutes(3));
        OverviewObservationData data = observe(
                query -> {
                    if (query.equals(OverviewPrometheusContract.flowFreshnessEpoch())) {
                        return List.of(sample(Map.of("coupon_id", "101"), staleAt.getEpochSecond()));
                    }
                    return happyInstant(query);
                },
                this::happyRange);

        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.STALE);
        assertThat(input(data, 101L).observedAt()).isEqualTo(staleAt);
        assertThat(input(data, 101L).completedCount()).isEqualTo(5d);
    }

    /** O1 질의 오류는 O1만 UNAVAILABLE로 만들고 이미 독립적인 O3·latency를 버리지 않습니다. */
    @Test
    @DisplayName("질의 오류를 관측 영역별로 격리한다")
    void isolatesFlowQueryFailure(CapturedOutput output) {
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> { throw new PromQueryException("flow failed"); });

        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.VALID);
        String warning = output.getOut().lines()
                .filter(line -> line.contains("Overview O1 Prometheus 질의 실패"))
                .findFirst()
                .orElseThrow();
        // DEBUG 로그의 상세 stacktrace와 분리해 WARN 행 자체가 한 줄인지 검증합니다.
        assertThat(warning).contains("flow failed")
                .doesNotContain("at com.kafkick.api.admin.observability.PromOverviewObservationSource");
    }

    /** 손상 range는 current·comparison·graph 모두의 단일 원천이므로 O1 전체를 격리합니다. */
    @Test
    @DisplayName("손상된 range matrix는 O1을 UNAVAILABLE로 만든다")
    void makesFlowUnavailableWhenRangeMatrixIsMalformed() {
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> { throw new PromQueryException("malformed expected range bucket"); });

        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 정의되지 않은 outcome label은 기존 업무 결과로 흡수하지 않고 O3를 UNAVAILABLE로 만듭니다. */
    @Test
    @DisplayName("unknown outcome label은 O3를 UNAVAILABLE로 만든다")
    void degradesUnknownOutcomeLabel() {
        OverviewObservationData data = observe(
                query -> query.equals(OverviewPrometheusContract.outcomes())
                        ? List.of(sample(Map.of("outcome", "NEW_RESULT"), 1d))
                        : happyInstant(query),
                this::happyRange);

        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.outcomeInput().counts()).isEmpty();
    }

    /** 상태 전이 거절은 서버 장애가 아니라 고객이 받을 수 없는 정책 결과로 집계합니다. */
    @Test
    @DisplayName("INVALID_TRANSITION outcome은 INELIGIBLE로 집계한다")
    void mapsInvalidTransitionOutcomeToIneligible() {
        OverviewObservationData data = observe(
                query -> query.equals(OverviewPrometheusContract.outcomes())
                        ? outcomeSamplesWithOnly("INVALID_TRANSITION", 1d)
                        : happyInstant(query),
                this::happyRange);

        double ineligible = data.outcomeInput().counts().stream()
                .filter(count -> count.type()
                        == AdminOverviewSnapshot.CustomerOutcomeType.INELIGIBLE)
                .mapToDouble(OutcomeCount::count)
                .sum();

        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(ineligible).isEqualTo(1d);
    }

    /** increase에 아직 나오지 않는 새 label도 snapshot inventory에서 먼저 차단해야 합니다. */
    @Test
    @DisplayName("increase에 없는 unknown outcome이 inventory에 있으면 O3는 UNAVAILABLE이다")
    void degradesUnknownOutcomePresentOnlyInInventory() {
        OverviewObservationData data = observe(query -> {
            if (query.equals(OverviewPrometheusContract.outcomeInventory())) {
                List<PromSample> inventory = new ArrayList<>(outcomeSamples(0d, 1d));
                inventory.add(sample(Map.of("outcome", "NEW_RESULT"), 1d));
                return List.copyOf(inventory);
            }
            return happyInstant(query);
        }, this::happyRange);

        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.outcomeInput().counts()).isEmpty();
    }

    /** fixed 14 series를 사전 등록했다는 모집단 증명 없이 누락을 0으로 보면 안 됩니다. */
    @Test
    @DisplayName("O3 snapshot inventory가 14 known label을 모두 증명하지 못하면 PENDING이다")
    void keepsIncompleteOutcomeInventoryPending() {
        OverviewObservationData data = observe(query -> {
            if (query.equals(OverviewPrometheusContract.outcomeInventory())) {
                List<PromSample> complete = outcomeSamples(0d, 1d);
                return complete.subList(0, complete.size() - 1);
            }
            return happyInstant(query);
        }, this::happyRange);

        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.PENDING);
        assertThat(data.outcomeInput().counts()).isEmpty();
    }

    /** O3 빈 시계열과 질의 실패를 각각 PENDING과 UNAVAILABLE로 구분합니다. */
    @Test
    @DisplayName("O3 empty와 query error를 구분한다")
    void distinguishesEmptyOutcomeFromOutcomeQueryFailure() {
        OverviewObservationData empty = observe(
                query -> query.equals(OverviewPrometheusContract.outcomes())
                        ? List.of() : happyInstant(query),
                this::happyRange);
        OverviewObservationData failed = observe(
                query -> {
                    if (query.equals(OverviewPrometheusContract.outcomes())) {
                        throw new PromQueryException("outcome failed");
                    }
                    return happyInstant(query);
                },
                this::happyRange);

        assertThat(empty.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.PENDING);
        assertThat(failed.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** attempt 발행 실패가 증가하면 관측된 attempt가 불완전하므로 STOPPED 근거로 쓰지 않습니다. */
    @Test
    @DisplayName("attempt publish failure 증가는 O1을 UNAVAILABLE로 만든다")
    void degradesFlowWhenAttemptPublishFailed() {
        OverviewObservationData data = observe(
                query -> query.equals(OverviewPrometheusContract.attemptPublishFailures())
                        ? List.of(sample(Map.of(), 1d)) : happyInstant(query),
                this::happyRange);

        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** failure 증가량 0도 그 시계열의 실제 scrape 시각이 확인된 경우에만 attempt 완전성을 보증합니다. */
    @Test
    @DisplayName("attempt publish failure freshness 누락과 stale은 attempt 상태를 허가하지 않는다")
    void requiresFreshAttemptPublishFailureGate() {
        String freshnessQueryPart = "timestamp("
                + OverviewPrometheusContract.ATTEMPT_PUBLISH_FAILURES_TOTAL;
        OverviewObservationData missing = observe(
                query -> query.contains(freshnessQueryPart) ? List.of() : happyInstant(query),
                this::happyRange);
        OverviewObservationData stale = observe(
                query -> query.contains(freshnessQueryPart)
                        ? List.of(sample(Map.of(), SNAPSHOT.minus(Duration.ofMinutes(3)).getEpochSecond()))
                        : happyInstant(query),
                this::happyRange);

        assertThat(input(missing, 101L).sourceStatus()).isEqualTo(SourceStatus.PENDING);
        assertThat(input(stale, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** reset-aware increase가 음수·NaN·무한대를 반환하면 0으로 정규화하지 않습니다. */
    @Test
    @DisplayName("음수와 비유한 increase는 유효 count가 아니다")
    void rejectsNegativeAndNonFiniteCounterIncreases() {
        for (double invalid : List.of(-1d, Double.NaN, Double.POSITIVE_INFINITY)) {
            OverviewObservationData data = observe(
                    this::happyInstant,
                    query -> List.of(
                            new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                    gridPointsWithLast(invalid)),
                            new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                    gridPoints(0d))));
            assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        }
    }

    /** Adapter 호출 계약 위반은 외부 Prometheus 장애처럼 UNAVAILABLE로 숨기지 않습니다. */
    @Test
    @DisplayName("내부 range 계약 위반은 관측 장애로 삼키지 않는다")
    void propagatesInternalRangeContractViolation() {
        assertThatThrownBy(() -> observe(
                this::happyInstant,
                query -> {
                    throw new IllegalArgumentException("내부 range 계약 위반");
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("내부 range 계약 위반");
    }

    /** O1은 fractional increase를 그대로 보존해야 ratio 임계에서 거짓 DECREASING을 만들지 않습니다. */
    @Test
    @DisplayName("O1 fractional increase와 실제 감소 비율을 그대로 보존한다")
    void preservesFractionalFlowAndRatioBoundary() {
        OverviewObservationData data = observe(
                this::happyInstant,
                query -> List.of(
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "attempt"),
                                gridPoints(1.01d)),
                        new PromRangeSeries(Map.of("coupon_id", "101", "stage", "success"),
                                gridPoints(List.of(
                                        0.99d, 0.99d, 0.99d, 0.99d, 0.99d, 0.99d,
                                        0.99d, 0.99d, 0.99d, 1.01d, 0.99d)))));

        IssuanceFlowInput flow = input(data, 101L);
        AdminOverviewSnapshot.IssuanceFlow calculated = new IssuanceFlowCalculator()
                .calculate(POLICY, List.of(flow)).issuanceFlows().get(101L).value();

        assertThat(flow.attemptedCount()).isEqualTo(1.01d);
        assertThat(flow.completedCount()).isEqualTo(0.99d);
        assertThat(flow.comparisonCompletedCount()).isEqualTo(1.01d);
        assertThat(flow.buckets().get(flow.buckets().size() - 2).completedCount()).isEqualTo(1.01d);
        assertThat(flow.buckets().getLast().completedCount()).isEqualTo(0.99d);
        assertThat(calculated.currentPerMinute()).isCloseTo(0.99d, within(1e-12));
        assertThat(calculated.state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** Prometheus reset-aware increase의 부동소수 추정치를 Core 계산까지 그대로 보존합니다. */
    @Test
    @DisplayName("O3 grouped increase의 fractional count를 계산기까지 보존한다")
    void preservesFractionalResetAwareOutcomeIncreaseThroughCalculator() {
        RecordingPromQuery instant = new RecordingPromQuery(query ->
                query.equals(OverviewPrometheusContract.outcomes())
                        ? outcomeSamplesWithFirst(0.1d) : happyInstant(query));

        OverviewObservationData data = new PromOverviewObservationSource(
                instant, new RecordingRangeQuery(this::happyRange),
                Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request());
        AdminOverviewSnapshot.CustomerOutcomeSummary calculated =
                new CustomerOutcomeCalculator()
                        .calculate(data.outcomeInput()).customerOutcomes().value();

        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(calculated.totalCount()).isEqualTo(104.1d);
        assertThat(calculated.outcomes()).first().satisfies(outcome -> {
            assertThat(outcome.type()).isEqualTo(AdminOverviewSnapshot.CustomerOutcomeType.ISSUED);
            assertThat(outcome.count()).isEqualTo(0.1d);
            assertThat(outcome.ratio()).isCloseTo(0.1d / 104.1d, within(1e-15));
        });
        assertThat(OverviewPrometheusContract.outcomes())
                .contains("sum by (outcome)", "increase(", "[5m]");
    }

    /** reset도 increase가 자체 반영하므로 adapter는 endpoint 뺄셈이나 resets gate를 두지 않습니다. */
    @Test
    @DisplayName("reset을 반영한 increase 값을 별도 resets 질의 없이 사용한다")
    void trustsResetAwareIncreaseWithoutSeparateResetGate() {
        RecordingPromQuery instant = new RecordingPromQuery(query ->
                query.equals(OverviewPrometheusContract.outcomes())
                        ? outcomeSamplesWithFirst(1.25d) : happyInstant(query));

        OverviewObservationData data = new PromOverviewObservationSource(
                instant, new RecordingRangeQuery(this::happyRange),
                Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request());

        assertThat(data.outcomeInput().counts())
                .filteredOn(count -> count.type() == AdminOverviewSnapshot.CustomerOutcomeType.ISSUED)
                .singleElement().extracting(OutcomeCount::count).isEqualTo(1.25d);
        assertThat(instant.queries).noneMatch(query -> query.contains("resets("));
    }

    /** replica 출현·소멸 사이의 aggregate endpoint 차분은 진짜 increase가 아닙니다. */
    @Test
    @DisplayName("replica churn은 endpoint 차분 대신 snapshot 한 번의 grouped increase로 관측한다")
    void usesSingleSnapshotIncreaseForReplicaChurnSemantics() {
        RecordingPromQuery instant = new RecordingPromQuery(this::happyInstant);
        new PromOverviewObservationSource(
                instant, new RecordingRangeQuery(this::happyRange),
                Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request());

        assertThat(instant.calls).filteredOn(call -> call.query().equals(
                        OverviewPrometheusContract.outcomes()))
                .containsExactly(new InstantCall(OverviewPrometheusContract.outcomes(), SNAPSHOT));
        assertThat(instant.calls).extracting(InstantCall::evaluationAt).containsOnly(SNAPSHOT);
    }

    /** fractional activity를 0으로 만들면 NO_TRAFFIC이 거짓으로 보고됩니다. */
    @Test
    @DisplayName("0보다 큰 fractional O3 activity는 NO_TRAFFIC이 아니다")
    void keepsPositiveFractionFromBecomingFalseNoTraffic() {
        OverviewObservationData data = observe(
                query -> query.equals(OverviewPrometheusContract.outcomes())
                        ? outcomeSamplesWithFirst(0.1d) : happyInstant(query), this::happyRange);

        assertThat(new CustomerOutcomeCalculator()
                .calculate(data.outcomeInput()).customerOutcomes().status())
                .isEqualTo(SourceStatus.VALID);
    }

    /** raw label 모집단 누락은 미배포와 실제 0을 구분할 수 없습니다. */
    @Test
    @DisplayName("O3 grouped increase의 raw label 모집단이 불완전하면 PENDING이다")
    void keepsIncompleteOutcomeIncreasePopulationPending() {
        OverviewObservationData data = observe(query -> {
            if (query.equals(OverviewPrometheusContract.outcomes())) {
                List<PromSample> complete = outcomeSamples(0d, 1d);
                return complete.subList(0, complete.size() - 1);
            }
            return happyInstant(query);
        }, this::happyRange);

        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.PENDING);
        assertThat(data.outcomeInput().counts()).isEmpty();
    }

    /** increase 결과는 비유한과 음수를 정상 건수로 노출하지 않습니다. */
    @Test
    @DisplayName("O3 grouped increase가 음수나 비유한이면 UNAVAILABLE이다")
    void makesInvalidOutcomeIncreaseUnavailable() {
        for (double invalid : List.of(-1d, Double.NaN, Double.POSITIVE_INFINITY)) {
            OverviewObservationData data = observe(
                    query -> query.equals(OverviewPrometheusContract.outcomes())
                            ? outcomeSamplesWithFirst(invalid) : happyInstant(query), this::happyRange);

            assertThat(data.outcomeInput().sourceStatus()).as("invalid increase " + invalid)
                    .isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(data.outcomeInput().counts()).isEmpty();
        }
    }

    /** 개별 raw 값이 유한해도 mapped type이나 전체 합이 Infinity면 O3 영역에 격리합니다. */
    @Test
    @DisplayName("O3 mapped 또는 total 합이 비유한이면 source에서 UNAVAILABLE이다")
    void isolatesNonFiniteMappedAndTotalOutcomeSumsAtSource() {
        List<Map<String, Double>> overflowing = List.of(
                Map.of("QUEUED", Double.MAX_VALUE, "QUEUE_REQUIRED", Double.MAX_VALUE),
                Map.of("ISSUED", Double.MAX_VALUE, "ALREADY_ISSUED", Double.MAX_VALUE));

        for (Map<String, Double> values : overflowing) {
            OverviewObservationData data = observe(
                    query -> query.equals(OverviewPrometheusContract.outcomes())
                            ? outcomeSamplesWithValues(values) : happyInstant(query), this::happyRange);

            assertThat(data.outcomeInput().sourceStatus()).as(values.toString())
                    .isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(new CustomerOutcomeCalculator().calculate(data.outcomeInput())
                    .customerOutcomes().status()).isEqualTo(SourceStatus.UNAVAILABLE);
            assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.VALID);
            assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.VALID);
        }
    }

    /** 성공 p99가 없거나 0이면 0ms를 만들지 않고 PENDING을 반환합니다. */
    @Test
    @DisplayName("성공 p99 공백과 0은 PENDING이다")
    void keepsLatencyPendingWhenSuccessfulP99IsAbsentOrZero() {
        OverviewObservationData absent = observe(
                query -> query.equals(OverviewPrometheusContract.successfulP99())
                        ? List.of() : happyInstant(query),
                this::happyRange);
        OverviewObservationData zero = observe(
                query -> query.equals(OverviewPrometheusContract.successfulP99())
                        ? List.of(sample(Map.of("instance", "api-1"), 0d)) : happyInstant(query),
                this::happyRange);

        assertThat(absent.latencySummary().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(zero.latencySummary().status()).isEqualTo(SourceStatus.PENDING);
    }

    /** 한 인스턴스의 p99가 비유한이면 남은 낮은 값만 max로 선택해 과소 보고할 수 없습니다. */
    @Test
    @DisplayName("valid와 비유한 p99가 섞이면 전체 latency는 PENDING이다")
    void keepsMixedInvalidLatencyVectorPending() {
        OverviewObservationData data = observe(
                query -> query.equals(OverviewPrometheusContract.successfulP99())
                        ? List.of(
                                sample(Map.of("instance", "api-1"), 0.4d),
                                sample(Map.of("instance", "api-2"), Double.POSITIVE_INFINITY))
                        : happyInstant(query),
                this::happyRange);

        assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(data.latencySummary().value()).isNull();
    }

    /** 나노초 변환 범위를 넘는 유한값도 포화된 Duration으로 노출하지 않습니다. */
    @Test
    @DisplayName("나노초 변환 범위를 넘는 p99는 latency만 UNAVAILABLE이다")
    void rejectsP99BeyondNanosecondConversionRange() {
        OverviewObservationData data = observe(
                query -> query.equals(OverviewPrometheusContract.successfulP99())
                        ? List.of(sample(Map.of("instance", "api-1"), Double.MAX_VALUE))
                        : happyInstant(query),
                this::happyRange);

        assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.latencySummary().value()).isNull();
        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.VALID);
    }

    /** 실제 timed HTTP parser의 p99 손상은 낮은 max로 축소되지 않고 latency에 격리됩니다. */
    @Test
    @DisplayName("실제 timed client의 mixed malformed p99는 latency만 UNAVAILABLE이다")
    void isolatesStrictTimedP99ParseFailureToLatency() {
        OverviewObservationData data = observeThroughRealTimedResponse(
                OverviewPrometheusContract.successfulP99(), """
                        {"status":"success","data":{"resultType":"vector","result":[
                          {"metric":{"instance":"api-1"},"value":[1755000000,"0.2"]},
                          {"metric":{"instance":"api-2"},"value":[1755000000,"broken"]}]}}
                        """);

        assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.VALID);
        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.VALID);
    }

    /** malformed unknown을 건너뛰 14 known만 남기는 실제 timed parser 경로를 O3 격리까지 검증합니다. */
    @Test
    @DisplayName("실제 timed client의 malformed unknown O3 표본은 O3만 UNAVAILABLE이다")
    void isolatesStrictTimedUnknownOutcomeParseFailureToO3() {
        OverviewObservationData data = observeThroughRealTimedResponse(
                OverviewPrometheusContract.outcomes(), """
                        {"status":"success","data":{"resultType":"vector","result":[
                          {"metric":{"outcome":"ISSUED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"QUEUED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"QUEUE_REQUIRED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"ALREADY_ISSUED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"STOCK_EXHAUSTED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"NOT_OPENED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"CAMPAIGN_CLOSED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"GRADE_NOT_ELIGIBLE"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"NO_ENTRY_TOKEN"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"ENTRY_TOKEN_EXPIRED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"TEMPORARILY_UNAVAILABLE"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"INTERNAL_ERROR"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"UNMAPPED"},"value":[1755000000,"1"]},
                          {"metric":{"outcome":"NEW_RESULT"},"value":[1755000000,"broken"]}]}}
                        """);

        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.VALID);
        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.VALID);
    }

    /** 첫 질의 뒤 예산이 만료되면 나머지 영역은 질의를 시작하지 않고 UNAVAILABLE로 끝납니다. */
    @Test
    @DisplayName("전체 예산 만료 뒤에는 후속 질의를 시작하지 않는다")
    void skipsLaterQueriesAfterTotalBudgetExpires() {
        AtomicInteger clockReads = new AtomicInteger();
        RecordingPromQuery instant = new RecordingPromQuery(this::happyInstant);
        RecordingRangeQuery range = new RecordingRangeQuery(this::happyRange);
        PromOverviewObservationSource source = new PromOverviewObservationSource(
                instant, range, Duration.ofMinutes(2), Duration.ofMillis(1),
                () -> clockReads.getAndIncrement() < 2 ? 0L : 2_000_000L);

        OverviewObservationData data = source.observe(request());

        assertThat(instant.queries).isEmpty();
        assertThat(range.queries).hasSize(1);
        assertThat(input(data, 101L).sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(data.latencySummary().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 정상 Overview instant query 응답을 질의 의미별로 제공합니다. */
    private List<PromSample> happyInstant(String query) {
        if (query.contains("timestamp(")
                && query.contains("app_kafka_attempt_publish_failures_total")) {
            return List.of(sample(Map.of(), SNAPSHOT.getEpochSecond()));
        }
        if (query.contains("timestamp(") && query.contains("app_issuance_flow_total")) {
            return couponSamples("", SNAPSHOT.getEpochSecond());
        }
        if (query.contains("app_issuance_event_last_success_epoch")) {
            return couponSamples("", SNAPSHOT.getEpochSecond());
        }
        if (query.contains("app_kafka_attempt_publish_failures_total")) {
            return List.of(sample(Map.of(), 0d));
        }
        if (query.contains("timestamp(") && query.contains("app_issuance_outcome_total")) {
            return List.of(sample(Map.of(), SNAPSHOT.getEpochSecond()));
        }
        if (query.contains("app_issuance_outcome_total")) {
            return outcomeSamples(0d, 1d);
        }
        if (query.contains("timestamp(") && query.contains("app_http_latency_seconds")) {
            return List.of(sample(Map.of(), SNAPSHOT.getEpochSecond()));
        }
        if (query.contains("app_http_latency_seconds")) {
            return List.of(
                    sample(Map.of("instance", "api-1"), 0.2d),
                    sample(Map.of("instance", "api-2"), 0.4d));
        }
        throw new AssertionError("예상하지 않은 instant query: " + query);
    }

    /** 14개 raw outcome의 increase 값을 base + (1-based index * increment)로 만듭니다. */
    private static List<PromSample> outcomeSamples(double base, double increment) {
        String[] labels = {
                "ISSUED", "QUEUED", "QUEUE_REQUIRED", "ALREADY_ISSUED", "STOCK_EXHAUSTED",
                "NOT_OPENED", "CAMPAIGN_CLOSED", "GRADE_NOT_ELIGIBLE", "NO_ENTRY_TOKEN",
                "ENTRY_TOKEN_EXPIRED", "TEMPORARILY_UNAVAILABLE", "INTERNAL_ERROR", "UNMAPPED",
                "INVALID_TRANSITION"
        };
        List<PromSample> samples = new ArrayList<>();
        for (int index = 0; index < labels.length; index++) {
            samples.add(sample(Map.of("outcome", labels[index]), base + ((index + 1) * increment)));
        }
        return List.copyOf(samples);
    }

    /** 첫 raw outcome 값만 바꿔 fractional·유효성 경계를 검증합니다. */
    private static List<PromSample> outcomeSamplesWithFirst(double firstValue) {
        List<PromSample> samples = new ArrayList<>(outcomeSamples(0d, 1d));
        samples.set(0, sample(Map.of("outcome", "ISSUED"), firstValue));
        return List.copyOf(samples);
    }

    /** 지정 raw outcome 값만 바꾼 합계 overflow fixture를 만듭니다. */
    private static List<PromSample> outcomeSamplesWithValues(Map<String, Double> replacements) {
        return outcomeSamples(0d, 1d).stream()
                .map(item -> replacements.containsKey(item.label("outcome"))
                        ? sample(Map.of("outcome", item.label("outcome")),
                                replacements.get(item.label("outcome")))
                        : item)
                .toList();
    }

    /** 지정 raw outcome 하나만 값으로 두고 나머지 사전 등록 label은 0으로 유지합니다. */
    private static List<PromSample> outcomeSamplesWithOnly(String target, double value) {
        return outcomeSamples(0d, 0d).stream()
                .map(item -> target.equals(item.label("outcome"))
                        ? sample(Map.of("outcome", target), value)
                        : item)
                .toList();
    }

    /** 두 OPEN 캠페인의 11-point matrix에 current·comparison endpoint를 명시합니다. */
    private List<PromRangeSeries> happyRange(String query) {
        List<PromRangeSeries> series = new ArrayList<>();
        for (String couponId : List.of("101", "102")) {
            double attempts = "101".equals(couponId) ? 2d : 1d;
            List<Double> successes = "101".equals(couponId)
                    ? List.of(1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 8d, 5d)
                    : List.of(1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 1d, 3d, 1d);
            series.add(new PromRangeSeries(Map.of("coupon_id", couponId, "stage", "attempt"),
                    gridPoints(attempts)));
            series.add(new PromRangeSeries(Map.of("coupon_id", couponId, "stage", "success"),
                    gridPoints(successes)));
        }
        return List.copyOf(series);
    }

    /** comparison와 current 평가점만 있는 불완전 history fixture를 만듭니다. */
    private static List<PromRangePoint> endpointPoints(double comparison, double current) {
        return List.of(
                new PromRangePoint(SNAPSHOT.minus(Duration.ofMinutes(1)), comparison),
                new PromRangePoint(SNAPSHOT, current));
    }

    /** query_range 폐구간의 11개 평가점에서 시작점은 이전값 문맥으로만 사용하는 fixture입니다. */
    private static List<PromRangePoint> gridPoints(double value) {
        List<PromRangePoint> points = new ArrayList<>();
        Instant start = SNAPSHOT.minus(Duration.ofMinutes(10));
        for (int index = 0; index <= 10; index++) {
            points.add(new PromRangePoint(start.plus(Duration.ofMinutes(index)), value));
        }
        return List.copyOf(points);
    }

    /** 11-point grid의 current endpoint만 바꿔 유효성 경계를 검증합니다. */
    private static List<PromRangePoint> gridPointsWithLast(double lastValue) {
        List<PromRangePoint> points = new ArrayList<>(gridPoints(1d));
        points.set(points.size() - 1, new PromRangePoint(SNAPSHOT, lastValue));
        return List.copyOf(points);
    }

    /** query_range의 11개 평가점에 서로 다른 값을 배치합니다. */
    private static List<PromRangePoint> gridPoints(List<Double> values) {
        if (values.size() != 11) {
            throw new IllegalArgumentException("grid fixture는 11개 값이 필요합니다.");
        }
        List<PromRangePoint> points = new ArrayList<>();
        Instant start = SNAPSHOT.minus(Duration.ofMinutes(10));
        for (int index = 0; index < values.size(); index++) {
            points.add(new PromRangePoint(start.plus(Duration.ofMinutes(index)), values.get(index)));
        }
        return List.copyOf(points);
    }

    /** 지정 평가점 하나만 제외한 불변 range fixture를 만듭니다. */
    private static List<PromRangePoint> withoutPoint(List<PromRangePoint> points, Instant excludedAt) {
        return points.stream().filter(point -> !point.observedAt().equals(excludedAt)).toList();
    }

    /** 두 캠페인 라벨을 가진 동일 값 표본을 만듭니다. */
    private static List<PromSample> couponSamples(String stage, double value) {
        if (stage.isEmpty()) {
            return List.of(
                    sample(Map.of("coupon_id", "101"), value),
                    sample(Map.of("coupon_id", "102"), value));
        }
        return List.of(
                sample(Map.of("coupon_id", "101", "stage", stage), value),
                sample(Map.of("coupon_id", "102", "stage", stage), value));
    }

    /** 실제 관측 시각과 혼동하지 않을 고정 evaluatedAt의 instant 표본을 만듭니다. */
    private static PromSample sample(Map<String, String> labels, double value) {
        return new PromSample("", labels, value, SNAPSHOT);
    }

    /** 결과에서 지정 캠페인의 O1 입력을 찾습니다. */
    private static IssuanceFlowInput input(OverviewObservationData data, long couponId) {
        return data.issuanceFlowInputs().stream()
                .filter(input -> input.couponId() == couponId)
                .findFirst()
                .orElseThrow();
    }

    /** 한 OPEN 캠페인의 고정 요청을 관측합니다. */
    private OverviewObservationData observe(
            Function<String, List<PromSample>> instantResponder,
            Function<String, List<PromRangeSeries>> rangeResponder
    ) {
        return observe(instantResponder, rangeResponder, request());
    }

    /** 지정 요청과 응답 대역을 함께 사용해 adapter 상태 경계를 관측합니다. */
    private OverviewObservationData observe(
            Function<String, List<PromSample>> instantResponder,
            Function<String, List<PromRangeSeries>> rangeResponder,
            OverviewObservationRequest request
    ) {
        return new PromOverviewObservationSource(
                new RecordingPromQuery(instantResponder), new RecordingRangeQuery(rangeResponder),
                Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request);
    }

    /** 하나의 target query만 실제 HTTP timed client로 통과시켜 parser와 source 격리를 함께 검증합니다. */
    private OverviewObservationData observeThroughRealTimedResponse(String targetQuery, String body) {
        RestClient.Builder builder = RestClient.builder().baseUrl("http://prometheus:9090");
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo(Matchers.containsString("/api/v1/query")))
                .andRespond(withSuccess(body, MediaType.APPLICATION_JSON));
        PromQueryClient realClient = new PromQueryClient(builder.build());
        PromTimeQuery selective = (query, evaluationAt) -> query.equals(targetQuery)
                ? realClient.query(query, evaluationAt) : happyInstant(query);

        OverviewObservationData data = new PromOverviewObservationSource(
                selective, new RecordingRangeQuery(this::happyRange),
                Duration.ofMinutes(2), Duration.ofSeconds(10)).observe(request());
        server.verify();
        return data;
    }

    /** adapter 경계 상태 시험에 사용할 한 OPEN 캠페인 요청입니다. */
    private static OverviewObservationRequest request() {
        return new OverviewObservationRequest(SNAPSHOT, List.of(
                new CampaignObservationTarget(
                        101L, CouponRoundStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT)), POLICY);
    }

    /** instant query를 기록하고 지정 응답 함수를 호출하는 작은 대역입니다. */
    private static final class RecordingPromQuery implements PromQuery, PromTimeQuery {
        private final BiFunction<String, Instant, List<PromSample>> responder;
        private final List<String> queries = new ArrayList<>();
        private final List<String> unanchoredQueries = new ArrayList<>();
        private final List<Instant> evaluationTimes = new ArrayList<>();
        private final List<InstantCall> calls = new ArrayList<>();

        /** 응답 함수를 보존합니다. */
        private RecordingPromQuery(Function<String, List<PromSample>> responder) {
            this((query, ignored) -> responder.apply(query));
        }

        /** 질의와 평가 시각을 모두 사용하는 응답 함수를 보존합니다. */
        private RecordingPromQuery(BiFunction<String, Instant, List<PromSample>> responder) {
            this.responder = responder;
        }

        /** 질의를 기록한 뒤 실제 adapter 입력 구조의 표본을 반환합니다. */
        @Override
        public List<PromSample> query(String promQl) {
            queries.add(promQl);
            unanchoredQueries.add(promQl);
            return responder.apply(promQl, null);
        }

        /** 평가 시각과 질의를 함께 기록해 snapshot anchor를 검증합니다. */
        @Override
        public List<PromSample> query(String promQl, Instant evaluationAt) {
            queries.add(promQl);
            evaluationTimes.add(evaluationAt);
            calls.add(new InstantCall(promQl, evaluationAt));
            return responder.apply(promQl, evaluationAt);
        }
    }

    /** range query를 기록하고 지정 응답 함수를 호출하는 작은 대역입니다. */
    private static final class RecordingRangeQuery implements PromRangeQuery {
        private final Function<String, List<PromRangeSeries>> responder;
        private final List<String> queries = new ArrayList<>();
        private final List<RangeCall> calls = new ArrayList<>();

        /** 응답 함수를 보존합니다. */
        private RecordingRangeQuery(Function<String, List<PromRangeSeries>> responder) {
            this.responder = responder;
        }

        /** 질의 문자열을 기록하고 range 표본을 반환합니다. */
        @Override
        public List<PromRangeSeries> query(String promQl, Instant start, Instant end, Duration step) {
            assertThat(Duration.between(start, end)).isEqualTo(Duration.ofMinutes(10));
            assertThat(step).isEqualTo(Duration.ofMinutes(1));
            queries.add(promQl);
            calls.add(new RangeCall(start, end, step));
            return responder.apply(promQl);
        }
    }

    /** range 호출의 정확한 시간 경계를 보존합니다. */
    private record RangeCall(Instant start, Instant end, Duration step) { }

    /** instant 질의와 평가 시각의 결합을 보존합니다. */
    private record InstantCall(String query, Instant evaluationAt) { }
}

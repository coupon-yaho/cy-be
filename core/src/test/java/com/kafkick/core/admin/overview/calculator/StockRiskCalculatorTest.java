package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.SourceStatus;

/** O4 V1 재고·O1 발급률 결합과 전역 부분 합계 금지 규칙을 검증합니다. */
class StockRiskCalculatorTest {

    private static final Instant NOW = Instant.parse("2026-08-22T03:00:00Z");
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.50, Duration.ofMinutes(3), Duration.ofMinutes(5), Duration.ofMinutes(2),
            Duration.ofMinutes(10));

    /** 잔여 3개와 분당 2.5개 발급은 72초 소진 예상이며 정책 기준 안의 위험입니다. */
    @Test
    void calculatesV1RemainingRatioAndCeilingDepletionEta() {
        StockRiskCalculator.StockRiskCalculation result = new StockRiskCalculator().calculate(POLICY, List.of(
                input(1L, 10L, 7L, flow(2.5, SourceStatus.VALID))));

        assertThat(result.stockForecasts().get(1L).value()).isEqualTo(
                new AdminOverviewSnapshot.StockForecast(3L, 10L, 0.3, Duration.ofSeconds(72)));
        assertThat(result.stockRisk().value()).isEqualTo(
                new AdminOverviewSnapshot.StockRiskSummary(1L, Duration.ofSeconds(72)));
    }

    /** 0·STALE·미관측 O1은 재고 수치와 별개로 소진 ETA를 계산하지 않습니다. */
    @Test
    void keepsStockButReturnsNullEtaForIneligibleIssuanceFlow() {
        StockRiskCalculator.StockRiskCalculation result = new StockRiskCalculator().calculate(POLICY, List.of(
                input(2L, 10L, 5L, flow(0.0, SourceStatus.VALID)),
                input(3L, 10L, 5L, flow(2.0, SourceStatus.STALE)),
                input(4L, 10L, 5L, new AdminOverviewSnapshot.Observation<>(
                        null, SourceStatus.UNAVAILABLE, null))));

        assertThat(result.stockForecasts().values())
                .extracting(observation -> observation.value().estimatedDepletion())
                .containsOnlyNulls();
        assertThat(result.stockRisk().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 재고 원천이 하나라도 없으면 정상 행은 보존하되 전체 소진 위험을 부분 합계로 반환하지 않습니다. */
    @Test
    void makesAggregateUnavailableWhenOneApplicableStockSourceIsUnavailable() {
        StockRiskCalculator.StockRiskCalculation result = new StockRiskCalculator().calculate(POLICY, List.of(
                input(5L, 10L, 5L, flow(1.0, SourceStatus.VALID)),
                new StockRiskCalculator.StockInput(6L, EngineVersion.V1, null, null,
                        SourceStatus.UNAVAILABLE, null, flow(1.0, SourceStatus.VALID))));

        assertThat(result.stockForecasts().get(5L).status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.stockRisk().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 역전·음수 수량과 전체 0은 실제 소진 0으로 보정하면 안 됩니다. */
    @Test
    void rejectsInvalidV1QuantityRange() {
        StockRiskCalculator calculator = new StockRiskCalculator();
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(7L, 0L, 0L, flow(1.0, SourceStatus.VALID)))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(8L, 10L, 11L, flow(1.0, SourceStatus.VALID)))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** O1 미수집은 보존하되 권위 재고가 확정된 V2는 V1과 같은 수량식으로 계산하는지 검증합니다. */
    @Test
    void preservesO1UnavailabilityAndCalculatesV2AuthoritativeStock() {
        StockRiskCalculator.StockRiskCalculation pendingRate = new StockRiskCalculator().calculate(POLICY, List.of(
                input(9L, 10L, 5L, new AdminOverviewSnapshot.Observation<>(
                        null, SourceStatus.PENDING, null))));
        StockRiskCalculator.StockRiskCalculation v2 = new StockRiskCalculator().calculate(POLICY, List.of(
                new StockRiskCalculator.StockInput(10L, EngineVersion.V2, 10L, 5L,
                        SourceStatus.VALID, NOW, flow(1.0, SourceStatus.VALID))));

        assertThat(pendingRate.stockRisk().status()).isEqualTo(SourceStatus.PENDING);
        assertThat(v2.stockForecasts().get(10L).status()).isEqualTo(SourceStatus.VALID);
        assertThat(v2.stockForecasts().get(10L).value().remainingQuantity()).isEqualTo(5L);
        assertThat(v2.stockRisk().status()).isEqualTo(SourceStatus.VALID);
    }

    /** N_A 전용·빈 모집단은 N_A이며, 0 rate는 관측된 비위험이지 미수집이 아닙니다. */
    @Test
    void excludesNotApplicableAndTreatsObservedZeroRateAsNonRisk() {
        StockRiskCalculator calculator = new StockRiskCalculator();
        StockRiskCalculator.StockRiskCalculation notApplicable = calculator.calculate(POLICY, List.of(
                new StockRiskCalculator.StockInput(11L, EngineVersion.V1, null, null,
                        SourceStatus.N_A, null, new AdminOverviewSnapshot.Observation<>(
                                null, SourceStatus.N_A, null))));
        StockRiskCalculator.StockRiskCalculation zero = calculator.calculate(POLICY, List.of(
                input(12L, 10L, 5L, flow(0.0, SourceStatus.NO_TRAFFIC))));

        assertThat(notApplicable.stockRisk().status()).isEqualTo(SourceStatus.N_A);
        assertThat(calculator.calculate(POLICY, List.of()).stockRisk().status()).isEqualTo(SourceStatus.N_A);
        assertThat(zero.stockRisk().value()).isEqualTo(new AdminOverviewSnapshot.StockRiskSummary(0L, null));
    }

    /** NaN·무한대 rate는 0초 ETA로 포화하지 않고 입력 계약 위반으로 거부합니다. */
    @Test
    void rejectsNonFiniteIssuanceRate() {
        StockRiskCalculator calculator = new StockRiskCalculator();
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(13L, 10L, 5L, flow(Double.NaN, SourceStatus.VALID)))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(14L, 10L, 5L, flow(Double.POSITIVE_INFINITY, SourceStatus.VALID)))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** O1 N_A는 위험 모집단에서 제외하고, NO_TRAFFIC rate는 정확히 0이며 의존 시각 중 최솟값을 쓴다. */
    @Test
    void excludesNotApplicableRateAndValidatesNoTrafficRateAndDependencyObservationTime() {
        StockRiskCalculator calculator = new StockRiskCalculator();
        StockRiskCalculator.StockRiskCalculation excluded = calculator.calculate(POLICY, List.of(
                input(15L, 10L, 5L, new AdminOverviewSnapshot.Observation<>(null, SourceStatus.N_A, null))));
        assertThat(excluded.stockRisk().status()).isEqualTo(SourceStatus.N_A);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(16L, 10L, 5L, flow(1.0, SourceStatus.NO_TRAFFIC)))))
                .isInstanceOf(IllegalArgumentException.class);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> oldFlow =
                new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.IssuanceFlow(1.0,
                        NOW.minusSeconds(2), NOW.minusSeconds(1), List.of(),
                        AdminOverviewSnapshot.IssuanceFlowState.NORMAL, null), SourceStatus.VALID,
                        NOW.minusSeconds(20));
        StockRiskCalculator.StockRiskCalculation timed = calculator.calculate(POLICY, List.of(
                input(17L, 10L, 5L, oldFlow)));
        assertThat(timed.stockRisk().observedAt()).isEqualTo(NOW.minusSeconds(20));
    }

    /** WARMING_UP은 ETA 없이 보존하고, ETA가 정책 경계와 같으면 위험에 포함합니다. */
    @Test
    void preservesWarmingUpAndIncludesExactDepletionThreshold() {
        StockRiskCalculator calculator = new StockRiskCalculator();
        assertThat(calculator.calculate(POLICY, List.of(input(18L, 10L, 5L, flow(1.0, SourceStatus.WARMING_UP))))
                .stockForecasts().get(18L).value().estimatedDepletion()).isNull();
        OverviewCalculationPolicy threshold = new OverviewCalculationPolicy(0.5, Duration.ofMinutes(1),
                Duration.ofMinutes(1), Duration.ofMinutes(1), Duration.ofSeconds(300));
        assertThat(calculator.calculate(threshold, List.of(input(19L, 10L, 5L, flow(1.0, SourceStatus.VALID))))
                .stockRisk().value().depletionRiskCount()).isEqualTo(1L);
    }

    /** STALE 단독 재고·O1 조합은 계산값은 유지하되 전역 최신성 상태를 STALE로 보존합니다. */
    @Test
    void preservesStaleAsTheOnlyAggregateStatus() {
        StockRiskCalculator.StockRiskCalculation stale = new StockRiskCalculator().calculate(POLICY, List.of(
                new StockRiskCalculator.StockInput(20L, EngineVersion.V1, 10L, 5L,
                        SourceStatus.STALE, NOW, flow(1.0, SourceStatus.VALID))));
        assertThat(stale.stockRisk().status()).isEqualTo(SourceStatus.STALE);
    }

    /** 재고 PENDING보다 O1 UNAVAILABLE이 높은 우선순위로 전역 위험에 보존됩니다. */
    @Test
    void prioritizesUnavailableIssuanceFlowOverPendingStock() {
        StockRiskCalculator.StockRiskCalculation result = new StockRiskCalculator().calculate(POLICY, List.of(
                new StockRiskCalculator.StockInput(23L, EngineVersion.V1, null, null,
                        SourceStatus.PENDING, null, new AdminOverviewSnapshot.Observation<>(
                                null, SourceStatus.UNAVAILABLE, null))));

        assertThat(result.stockRisk()).isEqualTo(
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null));
    }

    /** O1 N_A이면 재고 미수집 상태와 무관하게 해당 캠페인을 위험 모집단에서 제외합니다. */
    @Test
    void excludesNotApplicableIssuanceFlowWhenStockIsUnavailable() {
        StockRiskCalculator.StockRiskCalculation result = new StockRiskCalculator().calculate(POLICY, List.of(
                new StockRiskCalculator.StockInput(24L, EngineVersion.V1, null, null,
                        SourceStatus.UNAVAILABLE, null, new AdminOverviewSnapshot.Observation<>(
                                null, SourceStatus.N_A, null))));

        assertThat(result.stockRisk()).isEqualTo(
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.N_A, null));
    }

    /** O1 입력 자체가 없으면 재고 PENDING보다 높은 UNAVAILABLE로 전역 위험을 판정합니다. */
    @Test
    void treatsMissingIssuanceFlowAsUnavailableWithPendingStock() {
        StockRiskCalculator.StockRiskCalculation result = new StockRiskCalculator().calculate(POLICY, List.of(
                new StockRiskCalculator.StockInput(25L, EngineVersion.V1, null, null,
                        SourceStatus.PENDING, null, null)));

        assertThat(result.stockRisk()).isEqualTo(
                new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null));
    }

    /** 음수 V1 수량은 거부하며 ETA는 2^63초 이상이면 포화하지 않고 거부합니다. */
    @Test
    void rejectsNegativeQuantityAndEtaAtLongBoundary() {
        StockRiskCalculator calculator = new StockRiskCalculator();
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(21L, -1L, 0L, flow(1.0, SourceStatus.VALID))))).isInstanceOf(IllegalArgumentException.class);
        double boundaryRate = ((double) Long.MAX_VALUE * 60.0) / 0x1.0p63;
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(22L, Long.MAX_VALUE, 0L, flow(boundaryRate, SourceStatus.VALID)))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static StockRiskCalculator.StockInput input(
            long couponId, long totalQuantity, long activeCount,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> issuanceFlow
    ) {
        return new StockRiskCalculator.StockInput(couponId, EngineVersion.V1, totalQuantity, activeCount,
                SourceStatus.VALID, NOW, issuanceFlow);
    }

    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> flow(
            double perMinute, SourceStatus status
    ) {
        return new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.IssuanceFlow(
                perMinute, NOW.minusSeconds(1), NOW, List.of(),
                AdminOverviewSnapshot.IssuanceFlowState.NORMAL, null), status, NOW);
    }
}

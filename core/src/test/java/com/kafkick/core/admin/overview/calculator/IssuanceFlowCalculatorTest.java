package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.SourceStatus;

/** O1 발급률의 실제 경과 시간 보정과 상태 보존 규칙을 검증합니다. */
class IssuanceFlowCalculatorTest {

    private static final Instant END = Instant.parse("2026-08-22T03:00:00Z");
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.50, Duration.ofMinutes(3), Duration.ofMinutes(5), Duration.ofMinutes(2),
            Duration.ofMinutes(10));

    /** 현재율은 1분 count로 계산하고 그래프는 별도 10분 구간의 모든 버킷을 보존합니다. */
    @Test
    void separatesOneMinuteCurrentRateFromTenMinuteTrendWindow() {
        Instant currentStart = END.minus(Duration.ofMinutes(1));
        Instant trendStart = END.minus(Duration.ofMinutes(10));
        List<IssuanceFlowCalculator.IssuanceBucket> trendBuckets = java.util.stream.IntStream.range(0, 10)
                .mapToObj(index -> bucket(
                        trendStart.plus(Duration.ofMinutes(index)),
                        trendStart.plus(Duration.ofMinutes(index + 1L)),
                        1L))
                .toList();
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                40L, CouponRoundStatus.OPEN, true,
                currentStart, END, trendStart, END,
                2d, 2d, 4d,
                END.minus(Duration.ofMinutes(2)), currentStart,
                trendBuckets, END, currentStart,
                SourceStatus.VALID, END);

        AdminOverviewSnapshot.IssuanceFlow flow = new IssuanceFlowCalculator()
                .calculate(POLICY, List.of(input)).issuanceFlows().get(40L).value();

        assertThat(flow.currentPerMinute()).isEqualTo(2.0);
        assertThat(flow.windowStart()).isEqualTo(trendStart);
        assertThat(flow.windowEnd()).isEqualTo(END);
        assertThat(flow.points()).hasSize(10);
    }

    /** 30초 구간의 완료 3건은 분당 6건이고, 90초 버킷 9건도 분당 6건이어야 합니다. */
    @Test
    void calculatesRatesFromActualWindowAndBucketDurations() {
        IssuanceFlowCalculator.IssuanceFlowCalculation result = new IssuanceFlowCalculator().calculate(
                POLICY,
                List.of(input(7L, 10L, 9L, 20L, CouponRoundStatus.OPEN, true, SourceStatus.VALID,
                        END, END.minusSeconds(90), List.of(bucket(END.minusSeconds(90), END, 9L)))))
                ;

        AdminOverviewSnapshot.IssuanceFlow flow = result.issuanceFlows().get(7L).value();
        assertThat(result.issuanceFlows().get(7L).status()).isEqualTo(SourceStatus.VALID);
        assertThat(flow.currentPerMinute()).isEqualTo(6.0);
        assertThat(flow.points()).containsExactly(
                new AdminOverviewSnapshot.IssuanceRatePoint(END, 6.0));
        assertThat(flow.state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.DECREASING);
    }

    /** 수요가 없을 때 0은 실제 무트래픽이며 발급 중단 장애로 바뀌면 안 됩니다. */
    @Test
    void preservesNoTrafficInsteadOfClassifyingItAsStopped() {
        IssuanceFlowCalculator.IssuanceFlowCalculation result = new IssuanceFlowCalculator().calculate(
                POLICY,
                List.of(input(8L, 0L, 0L, 3L, CouponRoundStatus.OPEN, true, SourceStatus.VALID,
                        END, END.minus(Duration.ofMinutes(1)), List.of())));

        assertThat(result.issuanceFlows().get(8L).status()).isEqualTo(SourceStatus.NO_TRAFFIC);
        assertThat(result.issuanceFlows().get(8L).value().currentPerMinute()).isZero();
        assertThat(result.issuanceFlows().get(8L).value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** 서로 다른 모집단인 성공 수가 시도 수보다 커도 트래픽이 있는 정상 관측으로 보존합니다. */
    @Test
    void acceptsSuccessCountGreaterThanAttemptWithoutMarkingNoTraffic() {
        Instant windowStart = END.minus(Duration.ofMinutes(1));
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                36L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 0d, 1d, 1d,
                END.minus(Duration.ofMinutes(2)), windowStart,
                List.of(bucket(windowStart, END, 1L)), END,
                END.minus(Duration.ofMinutes(3)), SourceStatus.VALID, END);

        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> observation =
                new IssuanceFlowCalculator().calculate(POLICY, List.of(input)).issuanceFlows().get(36L);

        assertThat(observation.status()).isEqualTo(SourceStatus.VALID);
        assertThat(observation.value().currentPerMinute()).isEqualTo(1.0);
        assertThat(observation.value().state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** 명시적 연속 조건 시작 시각으로 중단 지속 시간을 계산하며 STALE 값도 수치 그대로 보존합니다. */
    @Test
    void calculatesStoppedDurationFromConditionStartAndPreservesStaleValue() {
        IssuanceFlowCalculator.IssuanceFlowCalculation result = new IssuanceFlowCalculator().calculate(
                POLICY,
                List.of(input(9L, 5L, 0L, 2L, CouponRoundStatus.OPEN, true, SourceStatus.STALE,
                        END, END.minus(Duration.ofMinutes(1)), List.of())));

        AdminOverviewSnapshot.IssuanceFlow flow = result.issuanceFlows().get(9L).value();
        assertThat(result.issuanceFlows().get(9L).status()).isEqualTo(SourceStatus.STALE);
        assertThat(flow.state()).isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.STOPPED);
        assertThat(flow.stateDuration()).isEqualTo(Duration.ofMinutes(3));
    }

    /** 역전 또는 0초 관측 구간은 0 rate로 위조하지 않고 계약 오류로 거부해야 합니다. */
    @Test
    void rejectsNonPositiveObservationWindow() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();

        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(input(
                10L, 1L, 1L, 1L, CouponRoundStatus.OPEN, true, SourceStatus.VALID,
                END, END, List.of()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 종료가 시작보다 앞선 역전 관측 구간도 계약 오류로 거부해야 합니다. */
    @Test
    void rejectsReversedObservationWindow() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                26L, CouponRoundStatus.OPEN, true, END, END.minusSeconds(1), END, END.minusSeconds(1),
                1d, 1d, 1d,
                END.minusSeconds(2), END.minusSeconds(1), List.of(), null,
                END.minus(Duration.ofMinutes(3)), SourceStatus.VALID, END))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 비교 구간은 raw count가 아니라 실제 분당 완료율로 비교해야 합니다. */
    @Test
    void comparesCurrentAndComparisonRatesRatherThanRawCounts() {
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                11L, CouponRoundStatus.OPEN, true, END.minusSeconds(30), END, END.minusSeconds(30), END,
                3d, 3d, 10d,
                END.minus(Duration.ofMinutes(5)), END, List.of(), END,
                END.minus(Duration.ofMinutes(3)), SourceStatus.VALID, END);

        assertThat(new IssuanceFlowCalculator().calculate(POLICY, List.of(input))
                .issuanceFlows().get(11L).value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** 값 없는 원천은 가짜 count·구간 없이 해당 상태를 정확히 전파해야 합니다. */
    @Test
    void preservesNonCarryingStatusWithoutInventedRawValues() {
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                12L, CouponRoundStatus.OPEN, null, null, null, null, null, null, null, null, null, null,
                null, null, null, SourceStatus.N_A, null);

        assertThat(new IssuanceFlowCalculator().calculate(POLICY, List.of(input))
                .issuanceFlows().get(12L))
                .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, SourceStatus.N_A, null));
    }

    /** condition 시작이 current 평가 구간보다 미래면 다른 규칙과 무관하게 계약 오류입니다. */
    @Test
    void rejectsConditionAfterCurrentWindowEndInIsolation() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                14L, CouponRoundStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END,
                END.minus(Duration.ofMinutes(1)), END, 1d, 1d, 1d,
                END.minus(Duration.ofMinutes(1)), END, List.of(), END,
                END.plusSeconds(1), SourceStatus.VALID, END))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 중첩 버킷은 다른 시간·완료 규칙을 모두 만족해도 계산 계약 오류입니다. */
    @Test
    void rejectsOverlappingBucketsInIsolation() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                15L, CouponRoundStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END,
                END.minus(Duration.ofMinutes(1)), END, 1d, 1d, 1d,
                END.minus(Duration.ofMinutes(1)), END, List.of(bucket(END.minusSeconds(40), END.minusSeconds(10), 1L),
                        bucket(END.minusSeconds(20), END, 1L)), END,
                END.minusSeconds(1), SourceStatus.VALID, END))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** raw scrape 시각은 snapshot 평가 구간 종료보다 과거일 수 있습니다. */
    @Test
    void acceptsRawObservationBeforeEvaluationWindowEnd() {
        assertThat(new IssuanceFlowCalculator.IssuanceFlowInput(
                16L, CouponRoundStatus.OPEN, true, END.minusSeconds(30), END, END.minusSeconds(30), END,
                1d, 0d, 1d,
                END.minusSeconds(30), END, List.of(), null, END.minusSeconds(1),
                SourceStatus.VALID, END.minusSeconds(1))).isNotNull();
    }

    /** 큰 유한 count의 1분 rate는 중간 곱셈 overflow 없이 유한한 그대로여야 합니다. */
    @Test
    void keepsMaximumFiniteCountRepresentableOverOneMinute() {
        Instant start = END.minus(Duration.ofMinutes(1));
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                41L, CouponRoundStatus.OPEN, true, start, END, start, END,
                Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                start.minus(Duration.ofMinutes(1)), start, List.of(), END, start,
                SourceStatus.VALID, END);

        double rate = new IssuanceFlowCalculator().calculate(POLICY, List.of(input))
                .issuanceFlows().get(41L).value().currentPerMinute();

        assertThat(rate).isFinite().isEqualTo(Double.MAX_VALUE);
    }

    /** Instant 전체 범위의 Duration도 nanos 변환 overflow 없이 유한 rate를 계산해야 합니다. */
    @Test
    void calculatesFiniteRateAcrossExtremeDuration() {
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                42L, CouponRoundStatus.OPEN, true, Instant.MIN, Instant.MAX, Instant.MIN, Instant.MAX,
                1d, 1d, 1d, Instant.MIN, Instant.MAX, List.of(), END, Instant.MIN,
                SourceStatus.VALID, END);

        double rate = new IssuanceFlowCalculator().calculate(POLICY, List.of(input))
                .issuanceFlows().get(42L).value().currentPerMinute();

        assertThat(rate).isFinite().isPositive();
    }

    /** 실제 분당 값이 double 범위를 넘는 current와 bucket은 Infinity로 공개하지 않습니다. */
    @Test
    void rejectsNonRepresentableCurrentAndBucketRates() {
        Instant nanosStart = END.minusNanos(1);
        IssuanceFlowCalculator.IssuanceFlowInput currentOverflow =
                new IssuanceFlowCalculator.IssuanceFlowInput(
                        43L, CouponRoundStatus.OPEN, true, nanosStart, END, nanosStart, END,
                        Double.MAX_VALUE, Double.MAX_VALUE, 1d,
                        END.minusSeconds(2), END.minusSeconds(1), List.of(), END, nanosStart,
                        SourceStatus.VALID, END);
        IssuanceFlowCalculator.IssuanceFlowInput bucketOverflow =
                new IssuanceFlowCalculator.IssuanceFlowInput(
                        44L, CouponRoundStatus.OPEN, true, END.minusSeconds(1), END,
                        END.minusSeconds(1), END,
                        1d, 1d, 1d, END.minusSeconds(2), END.minusSeconds(1),
                        List.of(bucket(nanosStart, END, Double.MAX_VALUE)), END,
                        END.minusSeconds(1), SourceStatus.VALID, END);

        assertThatThrownBy(() -> new IssuanceFlowCalculator().calculate(POLICY, List.of(currentOverflow)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("유한");
        assertThatThrownBy(() -> new IssuanceFlowCalculator().calculate(POLICY, List.of(bucketOverflow)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("유한");
    }

    /** 명시적 NO_TRAFFIC은 시도·완료 count가 모두 0이어야 하며 재고 0은 STOPPED가 아닙니다. */
    @Test
    void validatesExplicitNoTrafficAndDoesNotStopWhenStockIsEmpty() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                17L, CouponRoundStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END,
                END.minus(Duration.ofMinutes(1)), END, 1d, 1d, 1d,
                END.minus(Duration.ofMinutes(1)), END, List.of(), null, END.minusSeconds(1),
                SourceStatus.NO_TRAFFIC, END)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new IssuanceFlowCalculator().calculate(POLICY, List.of(input(
                18L, 1L, 0L, 1L, CouponRoundStatus.OPEN, false, SourceStatus.VALID, END,
                END.minus(Duration.ofMinutes(1)), List.of()))).issuanceFlows().get(18L).value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** PENDING·UNAVAILABLE은 값 없이 그대로 전파하고 역순 버킷은 시각순으로 노출합니다. */
    @Test
    void preservesUnavailableStatusesAndSortsBuckets() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        IssuanceFlowCalculator.IssuanceFlowInput pending = new IssuanceFlowCalculator.IssuanceFlowInput(
                19L, CouponRoundStatus.OPEN, null, null, null, null, null, null, null, null, null, null, null,
                null, null, SourceStatus.PENDING, null);
        IssuanceFlowCalculator.IssuanceFlowInput unordered = new IssuanceFlowCalculator.IssuanceFlowInput(
                20L, CouponRoundStatus.OPEN, true, END.minusSeconds(40), END, END.minusSeconds(40), END,
                2d, 2d, 2d,
                END.minusSeconds(40), END, List.of(bucket(END.minusSeconds(20), END, 1L),
                        bucket(END.minusSeconds(40), END.minusSeconds(20), 1L)), END,
                END.minusSeconds(1), SourceStatus.VALID, END);
        assertThat(calculator.calculate(POLICY, List.of(pending)).issuanceFlows().get(19L).status())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(calculator.calculate(POLICY, List.of(unordered)).issuanceFlows().get(20L).value().points())
                .extracting(AdminOverviewSnapshot.IssuanceRatePoint::observedAt)
                .containsExactly(END.minusSeconds(20), END);
    }

    /** 현재 NO_TRAFFIC이어도 과거 추세는 보존하고 추세 구간 밖 버킷만 거부합니다. */
    @Test
    void preservesHistoricalTrendForCurrentNoTrafficAndRejectsBucketOutsideTrendRange() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        IssuanceFlowCalculator.IssuanceFlowInput noTraffic = new IssuanceFlowCalculator.IssuanceFlowInput(
                21L, CouponRoundStatus.OPEN, true,
                END.minusSeconds(30), END, END.minus(Duration.ofMinutes(2)), END,
                0d, 0d, 0d, END.minus(Duration.ofMinutes(1)), END.minusSeconds(30),
                List.of(
                        bucket(END.minus(Duration.ofMinutes(2)), END.minus(Duration.ofMinutes(1)), 2L),
                        bucket(END.minus(Duration.ofMinutes(1)), END, 1L)),
                null, END.minusSeconds(1), SourceStatus.NO_TRAFFIC, END);

        assertThat(calculator.calculate(POLICY, List.of(noTraffic)).issuanceFlows().get(21L).value().points())
                .hasSize(2);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                23L, CouponRoundStatus.OPEN, true,
                END.minusSeconds(30), END, END.minusSeconds(30), END,
                1d, 1d, 1d, END.minusSeconds(30), END,
                List.of(bucket(END.minusSeconds(31), END.minusSeconds(1), 1L)), null,
                END.minusSeconds(1), SourceStatus.VALID, END)))).isInstanceOf(IllegalArgumentException.class);
    }

    /** UNAVAILABLE은 값 없이 정확히 전파하고 마지막 완료 시각이 미래이면 거부합니다. */
    @Test
    void preservesUnavailableAndRejectsFutureLastCompletion() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        assertThat(calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                24L, CouponRoundStatus.OPEN, null, null, null, null, null, null, null, null, null, null, null,
                null, null, SourceStatus.UNAVAILABLE, null))).issuanceFlows().get(24L).status())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                25L, CouponRoundStatus.OPEN, true, END.minusSeconds(1), END, END.minusSeconds(1), END,
                1d, 1d, 1d,
                END.minusSeconds(1), END, List.of(), END.plusSeconds(1), END.minusSeconds(1),
                SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 마지막 완료가 연속 무발급 시작 뒤라면 STOPPED 근거가 모순되므로 거부합니다. */
    @Test
    void rejectsLastCompletionAfterStoppedConditionStarted() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                27L, CouponRoundStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END,
                END.minus(Duration.ofMinutes(1)), END, 1d, 0d, 2d,
                END.minus(Duration.ofMinutes(2)), END.minus(Duration.ofMinutes(1)), List.of(),
                END.minusSeconds(30), END.minus(Duration.ofMinutes(12)), SourceStatus.VALID, END))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** trend 구간을 생략하는 과거 호환 생성자를 공개 API에 남기지 않습니다. */
    @Test
    void exposesOnlyCanonicalInputConstructorWithExplicitComparisonWindow() {
        assertThat(Arrays.stream(IssuanceFlowCalculator.IssuanceFlowInput.class.getDeclaredConstructors())
                .mapToInt(constructor -> constructor.getParameterCount()))
                .containsExactly(17);
    }

    /** 실제 완료 count와 lastCompletedAt은 같은 폐구간을 공유해야 거짓 STOPPED를 만들지 않습니다. */
    @Test
    void validatesLastCompletionAgainstCompletedCountWindowBoundaries() {
        Instant windowStart = END.minus(Duration.ofMinutes(1));
        Instant conditionStartedAt = END.minusSeconds(30);

        assertThat(new IssuanceFlowCalculator.IssuanceFlowInput(
                28L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 1d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart, conditionStartedAt,
                SourceStatus.VALID, END)).isNotNull();
        assertThat(new IssuanceFlowCalculator.IssuanceFlowInput(
                29L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 1d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), END, conditionStartedAt,
                SourceStatus.VALID, END)).isNotNull();
        assertThat(new IssuanceFlowCalculator.IssuanceFlowInput(
                30L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 0d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart.minusNanos(1),
                conditionStartedAt, SourceStatus.VALID, END)).isNotNull();
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                31L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 1d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), null, conditionStartedAt,
                SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                32L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 1d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart.minusNanos(1),
                conditionStartedAt, SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                33L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 0d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart, conditionStartedAt,
                SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                34L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 0d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), END, conditionStartedAt,
                SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 생성자를 통과한 무완료 원천은 정책 임계시간을 넘기면 STOPPED로 계산합니다. */
    @Test
    void appliesStoppedPolicyAfterCanonicalInputValidation() {
        Instant windowStart = END.minus(Duration.ofMinutes(1));
        OverviewCalculationPolicy shortStoppedPolicy = new OverviewCalculationPolicy(
                0.50, Duration.ofSeconds(20), Duration.ofMinutes(5), Duration.ofMinutes(2),
                Duration.ofMinutes(10));
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                35L, CouponRoundStatus.OPEN, true, windowStart, END, windowStart, END, 1d, 0d, 2d,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart.minusNanos(1),
                windowStart.plusSeconds(30), SourceStatus.VALID, END);

        IssuanceFlowCalculator.IssuanceFlowCalculation result =
                new IssuanceFlowCalculator().calculate(shortStoppedPolicy, List.of(input));

        assertThat(result.issuanceFlows().get(35L).value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.STOPPED);
    }

    private static IssuanceFlowCalculator.IssuanceFlowInput input(
            long couponId, double attemptedCount, double completedCount, double comparisonCompletedCount,
            CouponRoundStatus status, boolean stockAvailable, SourceStatus sourceStatus, Instant observedAt,
            Instant windowStart, List<IssuanceFlowCalculator.IssuanceBucket> buckets
    ) {
        return new IssuanceFlowCalculator.IssuanceFlowInput(
                couponId, status, stockAvailable, windowStart, observedAt, windowStart, observedAt,
                attemptedCount, completedCount,
                comparisonCompletedCount, windowStart.minus(Duration.ofMinutes(1)), windowStart, buckets,
                completedCount > 0d ? observedAt : null,
                observedAt.minus(Duration.ofMinutes(3)),
                sourceStatus, observedAt);
    }

    private static IssuanceFlowCalculator.IssuanceBucket bucket(
            Instant start, Instant end, double completedCount
    ) {
        return new IssuanceFlowCalculator.IssuanceBucket(start, end, completedCount);
    }
}

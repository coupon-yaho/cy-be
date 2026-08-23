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
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

/** O1 발급률의 실제 경과 시간 보정과 상태 보존 규칙을 검증합니다. */
class IssuanceFlowCalculatorTest {

    private static final Instant END = Instant.parse("2026-08-22T03:00:00Z");
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.50, Duration.ofMinutes(3), Duration.ofMinutes(5), Duration.ofMinutes(2),
            Duration.ofMinutes(10));

    /** 30초 구간의 완료 3건은 분당 6건이고, 90초 버킷 9건도 분당 6건이어야 합니다. */
    @Test
    void calculatesRatesFromActualWindowAndBucketDurations() {
        IssuanceFlowCalculator.IssuanceFlowCalculation result = new IssuanceFlowCalculator().calculate(
                POLICY,
                List.of(input(7L, 10L, 9L, 20L, CouponStatus.OPEN, true, SourceStatus.VALID,
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
                List.of(input(8L, 0L, 0L, 3L, CouponStatus.OPEN, true, SourceStatus.VALID,
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
                36L, CouponStatus.OPEN, true, windowStart, END, 0L, 1L, 1L,
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
                List.of(input(9L, 5L, 0L, 2L, CouponStatus.OPEN, true, SourceStatus.STALE,
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
                10L, 1L, 1L, 1L, CouponStatus.OPEN, true, SourceStatus.VALID,
                END, END, List.of()))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 종료가 시작보다 앞선 역전 관측 구간도 계약 오류로 거부해야 합니다. */
    @Test
    void rejectsReversedObservationWindow() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                26L, CouponStatus.OPEN, true, END, END.minusSeconds(1), 1L, 1L, 1L,
                END.minusSeconds(2), END.minusSeconds(1), List.of(), null,
                END.minus(Duration.ofMinutes(3)), SourceStatus.VALID, END))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 비교 구간은 raw count가 아니라 실제 분당 완료율로 비교해야 합니다. */
    @Test
    void comparesCurrentAndComparisonRatesRatherThanRawCounts() {
        IssuanceFlowCalculator.IssuanceFlowInput input = new IssuanceFlowCalculator.IssuanceFlowInput(
                11L, CouponStatus.OPEN, true, END.minusSeconds(30), END, 3L, 3L, 10L,
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
                12L, CouponStatus.OPEN, null, null, null, null, null, null, null, null,
                null, null, null, SourceStatus.N_A, null);

        assertThat(new IssuanceFlowCalculator().calculate(POLICY, List.of(input))
                .issuanceFlows().get(12L))
                .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, SourceStatus.N_A, null));
    }

    /** 미래 조건 시각과 중첩 버킷은 계약 오류입니다. */
    @Test
    void rejectsFutureConditionAndOverlappingBuckets() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                14L, CouponStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END, 1L, 1L, 1L,
                END.minus(Duration.ofMinutes(1)), END, List.of(), null, END.plusSeconds(1), SourceStatus.VALID, END))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                15L, CouponStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END, 1L, 1L, 1L,
                END.minus(Duration.ofMinutes(1)), END, List.of(bucket(END.minusSeconds(40), END.minusSeconds(10), 1L),
                        bucket(END.minusSeconds(20), END, 1L)), null, END.minusSeconds(1), SourceStatus.VALID, END))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 관측값이 구간 종료보다 과거이면 현재 rate의 기준 시각이 모순됩니다. */
    @Test
    void rejectsObservationBeforeWindowEnd() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                16L, CouponStatus.OPEN, true, END.minusSeconds(30), END, 1L, 1L, 1L,
                END.minusSeconds(30), END, List.of(), null, END.minusSeconds(1),
                SourceStatus.VALID, END.minusSeconds(1))).isInstanceOf(IllegalArgumentException.class);
    }

    /** 명시적 NO_TRAFFIC은 시도·완료 count가 모두 0이어야 하며 재고 0은 STOPPED가 아닙니다. */
    @Test
    void validatesExplicitNoTrafficAndDoesNotStopWhenStockIsEmpty() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                17L, CouponStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END, 1L, 1L, 1L,
                END.minus(Duration.ofMinutes(1)), END, List.of(), null, END.minusSeconds(1),
                SourceStatus.NO_TRAFFIC, END)).isInstanceOf(IllegalArgumentException.class);
        assertThat(new IssuanceFlowCalculator().calculate(POLICY, List.of(input(
                18L, 1L, 0L, 1L, CouponStatus.OPEN, false, SourceStatus.VALID, END,
                END.minus(Duration.ofMinutes(1)), List.of()))).issuanceFlows().get(18L).value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.NORMAL);
    }

    /** PENDING·UNAVAILABLE은 값 없이 그대로 전파하고 역순 버킷은 시각순으로 노출합니다. */
    @Test
    void preservesUnavailableStatusesAndSortsBuckets() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        IssuanceFlowCalculator.IssuanceFlowInput pending = new IssuanceFlowCalculator.IssuanceFlowInput(
                19L, CouponStatus.OPEN, null, null, null, null, null, null, null, null, null, null, null,
                SourceStatus.PENDING, null);
        IssuanceFlowCalculator.IssuanceFlowInput unordered = new IssuanceFlowCalculator.IssuanceFlowInput(
                20L, CouponStatus.OPEN, true, END.minusSeconds(40), END, 2L, 2L, 2L,
                END.minusSeconds(40), END, List.of(bucket(END.minusSeconds(20), END, 1L),
                        bucket(END.minusSeconds(40), END.minusSeconds(20), 1L)), END,
                END.minusSeconds(1), SourceStatus.VALID, END);
        assertThat(calculator.calculate(POLICY, List.of(pending)).issuanceFlows().get(19L).status())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(calculator.calculate(POLICY, List.of(unordered)).issuanceFlows().get(20L).value().points())
                .extracting(AdminOverviewSnapshot.IssuanceRatePoint::observedAt)
                .containsExactly(END.minusSeconds(20), END);
    }

    /** NO_TRAFFIC 그래프도 0이어야 하며 버킷 합계와 주 구간 밖 여부를 검증합니다. */
    @Test
    void rejectsInconsistentNoTrafficAndBucketTotalsOrRange() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                21L, CouponStatus.OPEN, true, END.minusSeconds(30), END, 0L, 0L, 0L,
                END.minusSeconds(30), END, List.of(bucket(END.minusSeconds(30), END, 1L)), null,
                END.minusSeconds(1), SourceStatus.NO_TRAFFIC, END)))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                22L, CouponStatus.OPEN, true, END.minusSeconds(30), END, 2L, 1L, 1L,
                END.minusSeconds(30), END, List.of(bucket(END.minusSeconds(30), END.minusSeconds(15), 1L),
                        bucket(END.minusSeconds(15), END, 1L)), null, END.minusSeconds(1), SourceStatus.VALID, END))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                23L, CouponStatus.OPEN, true, END.minusSeconds(30), END, 1L, 1L, 1L,
                END.minusSeconds(30), END, List.of(bucket(END.minusSeconds(31), END.minusSeconds(1), 1L)), null,
                END.minusSeconds(1), SourceStatus.VALID, END)))).isInstanceOf(IllegalArgumentException.class);
    }

    /** UNAVAILABLE은 값 없이 정확히 전파하고 마지막 완료 시각이 미래이면 거부합니다. */
    @Test
    void preservesUnavailableAndRejectsFutureLastCompletion() {
        IssuanceFlowCalculator calculator = new IssuanceFlowCalculator();
        assertThat(calculator.calculate(POLICY, List.of(new IssuanceFlowCalculator.IssuanceFlowInput(
                24L, CouponStatus.OPEN, null, null, null, null, null, null, null, null, null, null, null,
                SourceStatus.UNAVAILABLE, null))).issuanceFlows().get(24L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                25L, CouponStatus.OPEN, true, END.minusSeconds(1), END, 1L, 1L, 1L,
                END.minusSeconds(1), END, List.of(), END.plusSeconds(1), END.minusSeconds(1),
                SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 마지막 완료가 연속 무발급 시작 뒤라면 STOPPED 근거가 모순되므로 거부합니다. */
    @Test
    void rejectsLastCompletionAfterStoppedConditionStarted() {
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                27L, CouponStatus.OPEN, true, END.minus(Duration.ofMinutes(1)), END, 1L, 0L, 2L,
                END.minus(Duration.ofMinutes(2)), END.minus(Duration.ofMinutes(1)), List.of(),
                END.minusSeconds(30), END.minus(Duration.ofMinutes(12)), SourceStatus.VALID, END))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 비교 구간을 합성하는 13-인자 호환 생성자를 공개 API에 남기지 않습니다. */
    @Test
    void exposesOnlyCanonicalInputConstructorWithExplicitComparisonWindow() {
        assertThat(Arrays.stream(IssuanceFlowCalculator.IssuanceFlowInput.class.getDeclaredConstructors())
                .mapToInt(constructor -> constructor.getParameterCount()))
                .doesNotContain(13);
    }

    /** 실제 완료 count와 lastCompletedAt은 같은 폐구간을 공유해야 거짓 STOPPED를 만들지 않습니다. */
    @Test
    void validatesLastCompletionAgainstCompletedCountWindowBoundaries() {
        Instant windowStart = END.minus(Duration.ofMinutes(1));
        Instant conditionStartedAt = END.minusSeconds(30);

        assertThat(new IssuanceFlowCalculator.IssuanceFlowInput(
                28L, CouponStatus.OPEN, true, windowStart, END, 1L, 1L, 2L,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart, conditionStartedAt,
                SourceStatus.VALID, END)).isNotNull();
        assertThat(new IssuanceFlowCalculator.IssuanceFlowInput(
                29L, CouponStatus.OPEN, true, windowStart, END, 1L, 1L, 2L,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), END, conditionStartedAt,
                SourceStatus.VALID, END)).isNotNull();
        assertThat(new IssuanceFlowCalculator.IssuanceFlowInput(
                30L, CouponStatus.OPEN, true, windowStart, END, 1L, 0L, 2L,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart.minusNanos(1),
                conditionStartedAt, SourceStatus.VALID, END)).isNotNull();
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                31L, CouponStatus.OPEN, true, windowStart, END, 1L, 1L, 2L,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), null, conditionStartedAt,
                SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                32L, CouponStatus.OPEN, true, windowStart, END, 1L, 1L, 2L,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart.minusNanos(1),
                conditionStartedAt, SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                33L, CouponStatus.OPEN, true, windowStart, END, 1L, 0L, 2L,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart, conditionStartedAt,
                SourceStatus.VALID, END)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IssuanceFlowCalculator.IssuanceFlowInput(
                34L, CouponStatus.OPEN, true, windowStart, END, 1L, 0L, 2L,
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
                35L, CouponStatus.OPEN, true, windowStart, END, 1L, 0L, 2L,
                END.minus(Duration.ofMinutes(2)), windowStart, List.of(), windowStart.minusNanos(1),
                windowStart.plusSeconds(30), SourceStatus.VALID, END);

        IssuanceFlowCalculator.IssuanceFlowCalculation result =
                new IssuanceFlowCalculator().calculate(shortStoppedPolicy, List.of(input));

        assertThat(result.issuanceFlows().get(35L).value().state())
                .isEqualTo(AdminOverviewSnapshot.IssuanceFlowState.STOPPED);
    }

    private static IssuanceFlowCalculator.IssuanceFlowInput input(
            long couponId, long attemptedCount, long completedCount, long comparisonCompletedCount,
            CouponStatus status, boolean stockAvailable, SourceStatus sourceStatus, Instant observedAt,
            Instant windowStart, List<IssuanceFlowCalculator.IssuanceBucket> buckets
    ) {
        return new IssuanceFlowCalculator.IssuanceFlowInput(
                couponId, status, stockAvailable, windowStart, observedAt, attemptedCount, completedCount,
                comparisonCompletedCount, windowStart.minus(Duration.ofMinutes(1)), windowStart, buckets,
                completedCount > 0L ? observedAt : null,
                observedAt.minus(Duration.ofMinutes(3)),
                sourceStatus, observedAt);
    }

    private static IssuanceFlowCalculator.IssuanceBucket bucket(
            Instant start, Instant end, long completedCount
    ) {
        return new IssuanceFlowCalculator.IssuanceBucket(start, end, completedCount);
    }
}

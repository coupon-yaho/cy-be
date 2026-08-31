package com.kafkick.core.admin.overview.calculator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.observation.SourceStatus;

/** O2 대기열의 증감·ETA·부분 합계 금지 규칙을 검증합니다. */
class CouponRoundQueueCalculatorTest {

    private static final Instant NOW = Instant.parse("2026-08-22T03:00:00Z");
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.50, Duration.ofMinutes(3), Duration.ofMinutes(2), Duration.ofMinutes(2),
            Duration.ofMinutes(10));

    /** 11명 대기와 분당 4명 입장은 165초 ETA이며 안내 기준 초과로 판정됩니다. */
    @Test
    void calculatesIncreasingQueueAndCeilingEta() {
        CouponRoundQueueCalculator.QueueCalculation result = new CouponRoundQueueCalculator().calculate(
                POLICY, List.of(input(1L, 11L, 7L, 4L, SourceStatus.VALID)));

        AdminOverviewSnapshot.CouponRoundQueueStatus queue = result.queueStatuses().get(1L).value();
        assertThat(queue.trend()).isEqualTo(AdminOverviewSnapshot.TrendDirection.INCREASING);
        assertThat(queue.waitingDeltaPerMinute()).isEqualTo(4L);
        assertThat(queue.admissionsPerMinute()).isEqualTo(4.0);
        assertThat(queue.estimatedWait()).isEqualTo(Duration.ofSeconds(165));
        assertThat(queue.assessment())
                .isEqualTo(AdminOverviewSnapshot.CouponRoundQueueAssessment.GUIDANCE_THRESHOLD_EXCEEDED);
        assertThat(result.aggregateQueue().value().waitingCount()).isEqualTo(11L);
        assertThat(result.aggregateQueue().value().admissionsPerSecond())
                .isCloseTo(4.0 / 60.0, org.assertj.core.data.Offset.offset(0.000000000000001));
        assertThat(result.aggregateQueue().value().estimatedWait()).isEqualTo(Duration.ofSeconds(165));
    }

    /** 대기자가 있으나 입장률이 0이면 ETA를 무한대나 0으로 표시하지 않고 중단 후보를 만듭니다. */
    @Test
    void returnsNullEtaAndOneStalledActionWhenAdmissionsStopped() {
        CouponRoundQueueCalculator.QueueCalculation result = new CouponRoundQueueCalculator().calculate(
                POLICY, List.of(input(2L, 4L, 4L, 0L, SourceStatus.VALID)));

        AdminOverviewSnapshot.CouponRoundQueueStatus queue = result.queueStatuses().get(2L).value();
        assertThat(queue.estimatedWait()).isNull();
        assertThat(queue.assessment())
                .isEqualTo(AdminOverviewSnapshot.CouponRoundQueueAssessment.ADMISSION_STOPPED);
        assertThat(result.actionCandidates()).hasSize(1);
        assertThat(result.actionCandidates().getFirst().couponId()).isEqualTo(2L);
        assertThat(result.actionCandidates().getFirst().recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.QUEUE_STALLED);
    }

    /** 일부 적용 쿠폰 회차 원천이 없으면 계산 가능한 행만 남기고 전역 합계는 UNAVAILABLE입니다. */
    @Test
    void doesNotExposePartialAggregateWhenOneCouponRoundIsUnavailable() {
        CouponRoundQueueCalculator.QueueCalculation result = new CouponRoundQueueCalculator().calculate(
                POLICY, List.of(input(3L, 5L, 5L, 1L, SourceStatus.VALID),
                        input(4L, 8L, 7L, 1L, SourceStatus.UNAVAILABLE)));

        assertThat(result.queueStatuses().get(3L).status()).isEqualTo(SourceStatus.VALID);
        assertThat(result.queueStatuses().get(4L).status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.queueRisk().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(result.aggregateQueue().status()).isEqualTo(SourceStatus.UNAVAILABLE);
    }

    /** 각 행의 실제 경과시간 보정 rate 합은 입력 순서와 무관한 전체 입장률이어야 합니다. */
    @Test
    void sumsIndividuallyNormalizedAdmissionRatesAndKeepsOldestObservation() {
        CouponRoundQueueCalculator.QueueInput oneMinute = new CouponRoundQueueCalculator.QueueInput(
                5L, 90L, 90L, 60L, NOW.minus(Duration.ofMinutes(1)), NOW,
                NOW, null, SourceStatus.VALID, NOW.plusSeconds(10));
        CouponRoundQueueCalculator.QueueInput twoMinutes = new CouponRoundQueueCalculator.QueueInput(
                6L, 90L, 90L, 60L, NOW.minus(Duration.ofMinutes(2)), NOW,
                NOW, null, SourceStatus.VALID, NOW);

        CouponRoundQueueCalculator.QueueCalculation forward = new CouponRoundQueueCalculator().calculate(
                POLICY, List.of(oneMinute, twoMinutes));
        CouponRoundQueueCalculator.QueueCalculation reverse = new CouponRoundQueueCalculator().calculate(
                POLICY, List.of(twoMinutes, oneMinute));

        assertThat(forward.aggregateQueue().value().admissionsPerSecond()).isCloseTo(1.5, org.assertj.core.data.Offset.offset(0.000000000000001));
        assertThat(reverse.aggregateQueue().value()).isEqualTo(forward.aggregateQueue().value());
        assertThat(forward.aggregateQueue().observedAt()).isEqualTo(NOW);
    }

    /** N_A는 적용 모집단에서 제외하고, 빈·전부 N_A 전체값은 N_A로 반환해야 합니다. */
    @Test
    void excludesNotApplicableAndPreservesNonCarryingStatusesWithoutRawValues() {
        CouponRoundQueueCalculator.QueueInput notApplicable = new CouponRoundQueueCalculator.QueueInput(
                7L, null, null, null, null, null, null, null, SourceStatus.N_A, null);
        CouponRoundQueueCalculator.QueueCalculation onlyNotApplicable = new CouponRoundQueueCalculator().calculate(
                POLICY, List.of(notApplicable));

        assertThat(onlyNotApplicable.queueStatuses().get(7L).status()).isEqualTo(SourceStatus.N_A);
        assertThat(onlyNotApplicable.aggregateQueue().status()).isEqualTo(SourceStatus.N_A);
        assertThat(new CouponRoundQueueCalculator().calculate(POLICY, List.of()).queueRisk().status())
                .isEqualTo(SourceStatus.N_A);
    }

    /** STALE 값은 전역 계산에 포함하되 최신 정상값으로 바꾸지 않습니다. */
    @Test
    void preservesStaleAggregateStatus() {
        CouponRoundQueueCalculator.QueueCalculation stale = new CouponRoundQueueCalculator().calculate(
                POLICY, List.of(input(8L, 1L, 1L, 1L, SourceStatus.STALE)));
        assertThat(stale.aggregateQueue().status()).isEqualTo(SourceStatus.STALE);
    }

    /** 미래의 연속 무입장 시작 시각은 현재 판정 근거가 될 수 없습니다. */
    @Test
    void rejectsFutureConditionTime() {
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                9L, 1L, 1L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW, null, NOW.plusSeconds(1),
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 관측 시각은 입장 완료 count를 센 구간 종료보다 빠를 수 없습니다. */
    @Test
    void rejectsObservationBeforeWindowEnd() {
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                10L, 1L, 1L, 1L, NOW.minusSeconds(30), NOW, NOW.minusSeconds(1), null,
                SourceStatus.VALID, NOW.minusSeconds(1))).isInstanceOf(IllegalArgumentException.class);
    }

    /** 연속 무입장 시작 뒤의 마지막 입장은 중단 근거와 모순되므로 원천 경계에서 거부합니다. */
    @Test
    void validatesLastAdmissionAgainstAdmissionStoppedStart() {
        Instant stoppedStartedAt = NOW.minus(Duration.ofMinutes(12));

        assertThat(new CouponRoundQueueCalculator.QueueInput(
                26L, 4L, 4L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                stoppedStartedAt.minusSeconds(1), stoppedStartedAt, SourceStatus.VALID, NOW)).isNotNull();
        assertThat(new CouponRoundQueueCalculator.QueueInput(
                27L, 4L, 4L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                stoppedStartedAt, stoppedStartedAt, SourceStatus.VALID, NOW)).isNotNull();
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                28L, 4L, 4L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                NOW.minusSeconds(30), stoppedStartedAt, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                29L, 0L, 0L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                null, stoppedStartedAt, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                30L, 4L, 4L, 1L, NOW.minus(Duration.ofMinutes(1)), NOW,
                NOW.minusSeconds(30), stoppedStartedAt, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 실제 입장이 있으면 count 구간 양 끝의 마지막 입장 시각을 허용합니다. */
    @Test
    void acceptsLastAdmissionAtAdmissionCountWindowBoundaries() {
        Instant windowStart = NOW.minus(Duration.ofMinutes(1));
        Instant stoppedStartedAt = NOW.minusSeconds(30);

        assertThat(new CouponRoundQueueCalculator.QueueInput(
                31L, 1L, 1L, 1L, windowStart, NOW, windowStart, null, SourceStatus.VALID, NOW)).isNotNull();
        assertThat(new CouponRoundQueueCalculator.QueueInput(
                32L, 1L, 1L, 1L, windowStart, NOW, NOW, null, SourceStatus.VALID, NOW)).isNotNull();
        assertThat(new CouponRoundQueueCalculator.QueueInput(
                33L, 4L, 4L, 0L, windowStart, NOW, windowStart.minusNanos(1), stoppedStartedAt,
                SourceStatus.VALID, NOW)).isNotNull();
    }

    /** 입장 count와 마지막 입장 시각이 서로 모순되면 원천 경계에서 거부합니다. */
    @Test
    void rejectsLastAdmissionOutsideAdmissionCountWindow() {
        Instant windowStart = NOW.minus(Duration.ofMinutes(1));
        Instant stoppedStartedAt = NOW.minusSeconds(30);

        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                34L, 1L, 1L, 1L, windowStart, NOW, null, null, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                35L, 1L, 1L, 1L, windowStart, NOW, windowStart.minusNanos(1), null,
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                36L, 4L, 4L, 0L, windowStart, NOW, windowStart, stoppedStartedAt,
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                37L, 4L, 4L, 0L, windowStart, NOW, NOW, stoppedStartedAt,
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 생성자를 통과한 무입장 원천은 정책 임계시간을 넘기면 입장 중단으로 계산합니다. */
    @Test
    void appliesAdmissionStoppedPolicyAfterCanonicalInputValidation() {
        Instant windowStart = NOW.minus(Duration.ofMinutes(1));
        OverviewCalculationPolicy shortStoppedPolicy = new OverviewCalculationPolicy(
                0.50, Duration.ofMinutes(3), Duration.ofMinutes(2), Duration.ofSeconds(20),
                Duration.ofMinutes(10));
        CouponRoundQueueCalculator.QueueInput input = new CouponRoundQueueCalculator.QueueInput(
                38L, 4L, 4L, 0L, windowStart, NOW, windowStart.minusNanos(1),
                windowStart.plusSeconds(30), SourceStatus.VALID, NOW);

        CouponRoundQueueCalculator.QueueCalculation result =
                new CouponRoundQueueCalculator().calculate(shortStoppedPolicy, List.of(input));

        assertThat(result.queueStatuses().get(38L).value().assessment())
                .isEqualTo(AdminOverviewSnapshot.CouponRoundQueueAssessment.ADMISSION_STOPPED);
    }

    /** UNAVAILABLE은 PENDING보다 우선하고 stale 값은 확정 긴급 조치 후보가 될 수 없습니다. */
    @Test
    void prioritizesUnavailableAndCreatesActionOnlyForValidCurrentObservation() {
        CouponRoundQueueCalculator calculator = new CouponRoundQueueCalculator();
        CouponRoundQueueCalculator.QueueCalculation mixed = calculator.calculate(POLICY, List.of(
                new CouponRoundQueueCalculator.QueueInput(11L, null, null, null, null, null, null,
                        null, SourceStatus.PENDING, null),
                new CouponRoundQueueCalculator.QueueInput(12L, null, null, null, null, null, null,
                        null, SourceStatus.UNAVAILABLE, null)));
        CouponRoundQueueCalculator.QueueCalculation staleStopped = calculator.calculate(POLICY, List.of(
                input(13L, 2L, 2L, 0L, SourceStatus.STALE)));

        assertThat(mixed.aggregateQueue().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(staleStopped.actionCandidates()).isEmpty();
    }

    /** 명시적 NO_TRAFFIC은 실제 대기·입장 count도 모두 0이어야 합니다. */
    @Test
    void rejectsNonZeroCountsForExplicitNoTraffic() {
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                14L, 1L, 0L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW, null, null,
                SourceStatus.NO_TRAFFIC, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(
                15L, 0L, 0L, 1L, NOW.minus(Duration.ofMinutes(1)), NOW, NOW, null,
                SourceStatus.NO_TRAFFIC, NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 감소 추세와 대기 0의 정상 판정을 각각 계산합니다. */
    @Test
    void calculatesDecreasingTrendAndNormalEmptyQueue() {
        CouponRoundQueueCalculator calculator = new CouponRoundQueueCalculator();
        CouponRoundQueueCalculator.QueueCalculation result = calculator.calculate(POLICY, List.of(
                input(16L, 1L, 2L, 1L, SourceStatus.VALID),
                input(17L, 0L, 0L, 0L, SourceStatus.VALID)));
        assertThat(result.queueStatuses().get(16L).value().trend())
                .isEqualTo(AdminOverviewSnapshot.TrendDirection.DECREASING);
        assertThat(result.queueStatuses().get(17L).value().assessment())
                .isEqualTo(AdminOverviewSnapshot.CouponRoundQueueAssessment.NORMAL);
    }

    /** 적용 쿠폰 회차 하나가 PENDING이면 전체 대기 합계를 부분 정상값으로 노출하지 않습니다. */
    @Test
    void propagatesPendingToAggregateQueue() {
        CouponRoundQueueCalculator.QueueCalculation result = new CouponRoundQueueCalculator().calculate(POLICY, List.of(
                input(16L, 1L, 2L, 1L, SourceStatus.VALID),
                new CouponRoundQueueCalculator.QueueInput(18L, null, null, null, null, null, null,
                        null, SourceStatus.PENDING, null)));

        assertThat(result.aggregateQueue().status()).isEqualTo(SourceStatus.PENDING);
    }

    /** 음수 대기 수량은 원천 경계에서 거부합니다. */
    @Test
    void rejectsNegativeQueueCounts() {
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(19L, -1L, 0L, 0L,
                NOW.minus(Duration.ofMinutes(1)), NOW, null, NOW.minus(Duration.ofMinutes(1)),
                SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 쿠폰 회차별 대기 수량 합계가 long 범위를 넘으면 조용히 감싸지 않습니다. */
    @Test
    void rejectsAggregateQueueOverflow() {
        CouponRoundQueueCalculator calculator = new CouponRoundQueueCalculator();
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(20L, Long.MAX_VALUE, 0L, Long.MAX_VALUE, SourceStatus.VALID),
                input(21L, 1L, 0L, Long.MAX_VALUE, SourceStatus.VALID)))).isInstanceOf(ArithmeticException.class);
    }

    /** 현재·이전 대기 수가 같으면 변화 없음 추세를 반환합니다. */
    @Test
    void returnsUnchangedTrendForEqualQueueCounts() {
        assertThat(new CouponRoundQueueCalculator().calculate(POLICY, List.of(input(22L, 3L, 3L, 1L, SourceStatus.VALID)))
                .queueStatuses().get(22L).value().trend()).isEqualTo(AdminOverviewSnapshot.TrendDirection.UNCHANGED);
    }

    /** 0초 또는 역전된 입장 관측 구간은 속도 계산 전에 거부합니다. */
    @Test
    void rejectsNonPositiveAdmissionWindows() {
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(23L, 0L, 0L, 0L, NOW, NOW,
                null, NOW.minusSeconds(1), SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CouponRoundQueueCalculator.QueueInput(24L, 0L, 0L, 0L, NOW,
                NOW.minusSeconds(1), null, NOW.minusSeconds(2), SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    /** Long 상한 바로 다음 double 표현(2^63초 이상)의 ETA는 포화하지 않고 거부합니다. */
    @Test
    void rejectsEtaAtDoubleLongUpperBoundary() {
        assertThatThrownBy(() -> new CouponRoundQueueCalculator().calculate(POLICY, List.of(
                new CouponRoundQueueCalculator.QueueInput(25L, Long.MAX_VALUE, 0L, 1L,
                        NOW.minusSeconds(1).minusNanos(1), NOW, NOW, null,
                        SourceStatus.VALID, NOW)))).isInstanceOf(IllegalArgumentException.class);
    }

    private static CouponRoundQueueCalculator.QueueInput input(
            long couponId, long currentWaitingCount, long previousWaitingCount,
            long admittedCount, SourceStatus status
    ) {
        return new CouponRoundQueueCalculator.QueueInput(
                couponId, currentWaitingCount, previousWaitingCount, admittedCount,
                NOW.minus(Duration.ofMinutes(1)), NOW, admittedCount > 0L ? NOW : null,
                currentWaitingCount > 0L && admittedCount == 0L ? NOW.minus(Duration.ofMinutes(2)) : null,
                status, status.carriesValue() ? NOW : null);
    }
}

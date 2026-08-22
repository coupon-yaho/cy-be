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
class CampaignQueueCalculatorTest {

    private static final Instant NOW = Instant.parse("2026-08-22T03:00:00Z");
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.50, Duration.ofMinutes(3), Duration.ofMinutes(2), Duration.ofMinutes(2),
            Duration.ofMinutes(10));

    /** 11명 대기와 분당 4명 입장은 165초 ETA이며 안내 기준 초과로 판정됩니다. */
    @Test
    void calculatesIncreasingQueueAndCeilingEta() {
        CampaignQueueCalculator.QueueCalculation result = new CampaignQueueCalculator().calculate(
                POLICY, List.of(input(1L, 11L, 7L, 4L, SourceStatus.VALID)));

        AdminOverviewSnapshot.CampaignQueueStatus queue = result.queueStatuses().get(1L).value();
        assertThat(queue.trend()).isEqualTo(AdminOverviewSnapshot.TrendDirection.INCREASING);
        assertThat(queue.waitingDeltaPerMinute()).isEqualTo(4L);
        assertThat(queue.admissionsPerMinute()).isEqualTo(4.0);
        assertThat(queue.estimatedWait()).isEqualTo(Duration.ofSeconds(165));
        assertThat(queue.assessment())
                .isEqualTo(AdminOverviewSnapshot.CampaignQueueAssessment.GUIDANCE_THRESHOLD_EXCEEDED);
        assertThat(result.aggregateQueue().value()).isEqualTo(
                new AdminOverviewSnapshot.AggregateQueue(11L, 0.06666666666666667, Duration.ofSeconds(165)));
    }

    /** 대기자가 있으나 입장률이 0이면 ETA를 무한대나 0으로 표시하지 않고 중단 후보를 만듭니다. */
    @Test
    void returnsNullEtaAndOneStalledActionWhenAdmissionsStopped() {
        CampaignQueueCalculator.QueueCalculation result = new CampaignQueueCalculator().calculate(
                POLICY, List.of(input(2L, 4L, 4L, 0L, SourceStatus.VALID)));

        AdminOverviewSnapshot.CampaignQueueStatus queue = result.queueStatuses().get(2L).value();
        assertThat(queue.estimatedWait()).isNull();
        assertThat(queue.assessment())
                .isEqualTo(AdminOverviewSnapshot.CampaignQueueAssessment.ADMISSION_STOPPED);
        assertThat(result.actionCandidates()).hasSize(1);
        assertThat(result.actionCandidates().getFirst().couponId()).isEqualTo(2L);
        assertThat(result.actionCandidates().getFirst().recommendedAction().code())
                .isEqualTo(AdminOverviewSnapshot.ActionCode.QUEUE_STALLED);
    }

    /** 일부 적용 캠페인 원천이 없으면 계산 가능한 행만 남기고 전역 합계는 UNAVAILABLE입니다. */
    @Test
    void doesNotExposePartialAggregateWhenOneCampaignIsUnavailable() {
        CampaignQueueCalculator.QueueCalculation result = new CampaignQueueCalculator().calculate(
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
        CampaignQueueCalculator.QueueInput oneMinute = new CampaignQueueCalculator.QueueInput(
                5L, 90L, 90L, 60L, NOW.minus(Duration.ofMinutes(1)), NOW,
                NOW, null, SourceStatus.VALID, NOW.plusSeconds(10));
        CampaignQueueCalculator.QueueInput twoMinutes = new CampaignQueueCalculator.QueueInput(
                6L, 90L, 90L, 60L, NOW.minus(Duration.ofMinutes(2)), NOW,
                NOW, null, SourceStatus.VALID, NOW);

        CampaignQueueCalculator.QueueCalculation forward = new CampaignQueueCalculator().calculate(
                POLICY, List.of(oneMinute, twoMinutes));
        CampaignQueueCalculator.QueueCalculation reverse = new CampaignQueueCalculator().calculate(
                POLICY, List.of(twoMinutes, oneMinute));

        assertThat(forward.aggregateQueue().value().admissionsPerSecond()).isCloseTo(1.5, org.assertj.core.data.Offset.offset(0.000000000000001));
        assertThat(reverse.aggregateQueue().value()).isEqualTo(forward.aggregateQueue().value());
        assertThat(forward.aggregateQueue().observedAt()).isEqualTo(NOW);
    }

    /** N_A는 적용 모집단에서 제외하고, 빈·전부 N_A 전체값은 N_A로 반환해야 합니다. */
    @Test
    void excludesNotApplicableAndPreservesNonCarryingStatusesWithoutRawValues() {
        CampaignQueueCalculator.QueueInput notApplicable = new CampaignQueueCalculator.QueueInput(
                7L, null, null, null, null, null, null, null, SourceStatus.N_A, null);
        CampaignQueueCalculator.QueueCalculation onlyNotApplicable = new CampaignQueueCalculator().calculate(
                POLICY, List.of(notApplicable));

        assertThat(onlyNotApplicable.queueStatuses().get(7L).status()).isEqualTo(SourceStatus.N_A);
        assertThat(onlyNotApplicable.aggregateQueue().status()).isEqualTo(SourceStatus.N_A);
        assertThat(new CampaignQueueCalculator().calculate(POLICY, List.of()).queueRisk().status())
                .isEqualTo(SourceStatus.N_A);
    }

    /** STALE 값은 전역 계산에 포함하되 전역 상태로 숨기지 않고, 미래 중단 시작 시각은 거부합니다. */
    @Test
    void preservesStaleAggregateStatusAndRejectsFutureConditionTime() {
        CampaignQueueCalculator.QueueCalculation stale = new CampaignQueueCalculator().calculate(
                POLICY, List.of(input(8L, 1L, 1L, 1L, SourceStatus.STALE)));
        assertThat(stale.aggregateQueue().status()).isEqualTo(SourceStatus.STALE);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                9L, 1L, 1L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW, null, NOW.plusSeconds(1),
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 관측 시각은 입장 완료 count를 센 구간 종료보다 빠를 수 없습니다. */
    @Test
    void rejectsObservationBeforeWindowEnd() {
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                10L, 1L, 1L, 1L, NOW.minusSeconds(30), NOW, NOW.minusSeconds(1), null,
                SourceStatus.VALID, NOW.minusSeconds(1))).isInstanceOf(IllegalArgumentException.class);
    }

    /** 연속 무입장 시작 뒤의 마지막 입장은 중단 근거와 모순되므로 원천 경계에서 거부합니다. */
    @Test
    void validatesLastAdmissionAgainstAdmissionStoppedStart() {
        Instant stoppedStartedAt = NOW.minus(Duration.ofMinutes(12));

        assertThat(new CampaignQueueCalculator.QueueInput(
                26L, 4L, 4L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                stoppedStartedAt.minusSeconds(1), stoppedStartedAt, SourceStatus.VALID, NOW)).isNotNull();
        assertThat(new CampaignQueueCalculator.QueueInput(
                27L, 4L, 4L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                stoppedStartedAt, stoppedStartedAt, SourceStatus.VALID, NOW)).isNotNull();
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                28L, 4L, 4L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                NOW.minusSeconds(30), stoppedStartedAt, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                29L, 0L, 0L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW,
                null, stoppedStartedAt, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                30L, 4L, 4L, 1L, NOW.minus(Duration.ofMinutes(1)), NOW,
                NOW.minusSeconds(30), stoppedStartedAt, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 실제 입장 count와 lastAdmissionAt은 같은 폐구간을 공유해야 거짓 중단을 만들지 않습니다. */
    @Test
    void validatesLastAdmissionAgainstAdmissionCountWindowBoundaries() {
        Instant windowStart = NOW.minus(Duration.ofMinutes(1));
        Instant stoppedStartedAt = NOW.minusSeconds(30);

        assertThat(new CampaignQueueCalculator.QueueInput(
                31L, 1L, 1L, 1L, windowStart, NOW, windowStart, null, SourceStatus.VALID, NOW)).isNotNull();
        assertThat(new CampaignQueueCalculator.QueueInput(
                32L, 1L, 1L, 1L, windowStart, NOW, NOW, null, SourceStatus.VALID, NOW)).isNotNull();
        assertThat(new CampaignQueueCalculator.QueueInput(
                33L, 4L, 4L, 0L, windowStart, NOW, windowStart.minusNanos(1), stoppedStartedAt,
                SourceStatus.VALID, NOW)).isNotNull();
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                34L, 1L, 1L, 1L, windowStart, NOW, null, null, SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                35L, 1L, 1L, 1L, windowStart, NOW, windowStart.minusNanos(1), null,
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                36L, 4L, 4L, 0L, windowStart, NOW, windowStart, stoppedStartedAt,
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                37L, 4L, 4L, 0L, windowStart, NOW, NOW, stoppedStartedAt,
                SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
        OverviewCalculationPolicy shortStoppedPolicy = new OverviewCalculationPolicy(
                0.50, Duration.ofMinutes(3), Duration.ofMinutes(2), Duration.ofSeconds(20),
                Duration.ofMinutes(10));
        assertThatThrownBy(() -> new CampaignQueueCalculator().calculate(shortStoppedPolicy, List.of(
                new CampaignQueueCalculator.QueueInput(
                        38L, 4L, 4L, 0L, windowStart, NOW, windowStart.plusSeconds(15),
                        windowStart.plusSeconds(30), SourceStatus.VALID, NOW))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** UNAVAILABLE은 PENDING보다 우선하고 stale 값은 확정 긴급 조치 후보가 될 수 없습니다. */
    @Test
    void prioritizesUnavailableAndCreatesActionOnlyForValidCurrentObservation() {
        CampaignQueueCalculator calculator = new CampaignQueueCalculator();
        CampaignQueueCalculator.QueueCalculation mixed = calculator.calculate(POLICY, List.of(
                new CampaignQueueCalculator.QueueInput(11L, null, null, null, null, null, null,
                        null, SourceStatus.PENDING, null),
                new CampaignQueueCalculator.QueueInput(12L, null, null, null, null, null, null,
                        null, SourceStatus.UNAVAILABLE, null)));
        CampaignQueueCalculator.QueueCalculation staleStopped = calculator.calculate(POLICY, List.of(
                input(13L, 2L, 2L, 0L, SourceStatus.STALE)));

        assertThat(mixed.aggregateQueue().status()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(staleStopped.actionCandidates()).isEmpty();
    }

    /** 명시적 NO_TRAFFIC은 실제 대기·입장 count도 모두 0이어야 합니다. */
    @Test
    void rejectsNonZeroCountsForExplicitNoTraffic() {
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                14L, 1L, 0L, 0L, NOW.minus(Duration.ofMinutes(1)), NOW, null, null,
                SourceStatus.NO_TRAFFIC, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(
                15L, 0L, 0L, 1L, NOW.minus(Duration.ofMinutes(1)), NOW, NOW, null,
                SourceStatus.NO_TRAFFIC, NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    /** 감소·변화 없음 추세, 대기 0 미중단, 음수·합계 overflow와 PENDING 전파를 고정합니다. */
    @Test
    void coversTrendAndNumericBoundaries() {
        CampaignQueueCalculator calculator = new CampaignQueueCalculator();
        CampaignQueueCalculator.QueueCalculation result = calculator.calculate(POLICY, List.of(
                input(16L, 1L, 2L, 1L, SourceStatus.VALID), input(17L, 0L, 0L, 0L, SourceStatus.VALID),
                new CampaignQueueCalculator.QueueInput(18L, null, null, null, null, null, null,
                        null, SourceStatus.PENDING, null)));
        assertThat(result.queueStatuses().get(16L).value().trend())
                .isEqualTo(AdminOverviewSnapshot.TrendDirection.DECREASING);
        assertThat(result.queueStatuses().get(17L).value().assessment())
                .isEqualTo(AdminOverviewSnapshot.CampaignQueueAssessment.NORMAL);
        assertThat(result.aggregateQueue().status()).isEqualTo(SourceStatus.PENDING);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(19L, -1L, 0L, 0L,
                NOW.minus(Duration.ofMinutes(1)), NOW, null, NOW.minus(Duration.ofMinutes(1)),
                SourceStatus.VALID, NOW))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.calculate(POLICY, List.of(
                input(20L, Long.MAX_VALUE, 0L, Long.MAX_VALUE, SourceStatus.VALID),
                input(21L, 1L, 0L, Long.MAX_VALUE, SourceStatus.VALID)))).isInstanceOf(ArithmeticException.class);
    }

    /** 0초·역전 구간은 거부하고 동일 count는 UNCHANGED 추세를 반환합니다. */
    @Test
    void rejectsNonPositiveWindowsAndReturnsUnchangedTrend() {
        assertThat(new CampaignQueueCalculator().calculate(POLICY, List.of(input(22L, 3L, 3L, 1L, SourceStatus.VALID)))
                .queueStatuses().get(22L).value().trend()).isEqualTo(AdminOverviewSnapshot.TrendDirection.UNCHANGED);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(23L, 0L, 0L, 0L, NOW, NOW,
                null, NOW.minusSeconds(1), SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignQueueCalculator.QueueInput(24L, 0L, 0L, 0L, NOW,
                NOW.minusSeconds(1), null, NOW.minusSeconds(2), SourceStatus.VALID, NOW)).isInstanceOf(IllegalArgumentException.class);
    }

    /** Long 상한 바로 다음 double 표현(2^63초 이상)의 ETA는 포화하지 않고 거부합니다. */
    @Test
    void rejectsEtaAtDoubleLongUpperBoundary() {
        assertThatThrownBy(() -> new CampaignQueueCalculator().calculate(POLICY, List.of(
                new CampaignQueueCalculator.QueueInput(25L, Long.MAX_VALUE, 0L, 1L,
                        NOW.minusSeconds(1).minusNanos(1), NOW, NOW, null,
                        SourceStatus.VALID, NOW)))).isInstanceOf(IllegalArgumentException.class);
    }

    private static CampaignQueueCalculator.QueueInput input(
            long couponId, long currentWaitingCount, long previousWaitingCount,
            long admittedCount, SourceStatus status
    ) {
        return new CampaignQueueCalculator.QueueInput(
                couponId, currentWaitingCount, previousWaitingCount, admittedCount,
                NOW.minus(Duration.ofMinutes(1)), NOW, admittedCount > 0L ? NOW : null,
                currentWaitingCount > 0L && admittedCount == 0L ? NOW.minus(Duration.ofMinutes(2)) : null,
                status, status.carriesValue() ? NOW : null);
    }
}

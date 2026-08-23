package com.kafkick.core.admin.overview.observation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

/** 관리자 운영현황 관측 조회 계약의 모집단·불변 목록·상태 규칙을 검증합니다. */
class OverviewObservationContractTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-23T03:00:00Z");
    private static final Instant WINDOW_START = SNAPSHOT_AT.minus(Duration.ofMinutes(1));
    private static final OverviewCalculationPolicy POLICY = new OverviewCalculationPolicy(
            0.5, Duration.ofMinutes(2), Duration.ofMinutes(3),
            Duration.ofMinutes(2), Duration.ofMinutes(5));

    /** 관측 원천이 Core와 같은 감소·중단 판정 기준으로 연속 조건을 도출하도록 정책을 보존합니다. */
    @Test
    void requestRequiresCalculationPolicy() {
        OverviewObservationRequest request = new OverviewObservationRequest(
                SNAPSHOT_AT, List.of(target(10L)), POLICY);

        assertThat(request.policy()).isSameAs(POLICY);
        assertThatThrownBy(() -> new OverviewObservationRequest(
                SNAPSHOT_AT, List.of(target(10L)), null))
                .isInstanceOf(NullPointerException.class);
    }

    /** 요청은 원본 대상 목록 변경과 외부 수정을 차단하고 유효한 고유 캠페인만 받습니다. */
    @Test
    void requestDefensivelyCopiesUniqueCampaignTargets() {
        List<CampaignObservationTarget> targets = new ArrayList<>();
        targets.add(target(11L));

        OverviewObservationRequest request = new OverviewObservationRequest(SNAPSHOT_AT, targets, POLICY);

        targets.clear();

        assertThat(request.campaignTargets()).containsExactly(target(11L));
        assertThatThrownBy(() -> request.campaignTargets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new OverviewObservationRequest(SNAPSHOT_AT, null, POLICY))
                .isInstanceOf(NullPointerException.class);
        List<CampaignObservationTarget> targetsWithNull = new ArrayList<>();
        targetsWithNull.add(null);
        assertThatThrownBy(() -> new OverviewObservationRequest(SNAPSHOT_AT, targetsWithNull, POLICY))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewObservationRequest(SNAPSHOT_AT,
                List.of(target(11L), target(11L)), POLICY))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                0L, CouponStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                -1L, CouponStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Snapshot 이후에 관측한 재고가 현재 O1 판정에 섞이는 회귀를 요청 생성 시 차단합니다. */
    @Test
    void requestRejectsValueCarryingStockObservedAfterSnapshot() {
        CampaignObservationTarget futureStock = new CampaignObservationTarget(
                12L, CouponStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT_AT.plusNanos(1L));

        assertThatThrownBy(() -> new OverviewObservationRequest(
                SNAPSHOT_AT, List.of(futureStock), POLICY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stockObservedAt");
    }

    /** 데이터는 어떤 O1·O3·전체 관측 입력도 null로 생략할 수 없습니다. */
    @Test
    void dataRejectsNullInputs() {
        OverviewObservationRequest request = request(36L);
        List<IssuanceFlowCalculator.IssuanceFlowInput> inputs = List.of(flowInput(36L, 1L, 1L));
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateIssuanceRate> issuanceRate =
                aggregateIssuanceRate(SourceStatus.VALID);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latency =
                latencySummary(SourceStatus.VALID);

        assertThatThrownBy(() -> new OverviewObservationData(null, inputs, outcomeInput(), issuanceRate, latency))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewObservationData(request, null, outcomeInput(), issuanceRate, latency))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewObservationData(request, inputs, null, issuanceRate, latency))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewObservationData(request, inputs, outcomeInput(), null, latency))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OverviewObservationData(request, inputs, outcomeInput(), issuanceRate, null))
                .isInstanceOf(NullPointerException.class);
        List<IssuanceFlowCalculator.IssuanceFlowInput> inputsWithNull = new ArrayList<>();
        inputsWithNull.add(null);
        assertThatThrownBy(() -> new OverviewObservationData(request, inputsWithNull, outcomeInput(), issuanceRate,
                latency)).isInstanceOf(NullPointerException.class);
    }

    /** 진행 캠페인은 재고 값 또는 미관측 상태를 보존하고 비진행 캠페인은 N_A만 허용합니다. */
    @Test
    void targetRequiresStockAvailabilityOnlyForOpenCampaigns() {
        CampaignObservationTarget pending = new CampaignObservationTarget(
                11L, CouponStatus.OPEN, null, SourceStatus.PENDING);
        CampaignObservationTarget unavailable = new CampaignObservationTarget(
                12L, CouponStatus.OPEN, null, SourceStatus.UNAVAILABLE);

        assertThat(pending.stockAvailable()).isNull();
        assertThat(pending.stockStatus()).isEqualTo(SourceStatus.PENDING);
        assertThat(unavailable.stockAvailable()).isNull();
        assertThat(unavailable.stockStatus()).isEqualTo(SourceStatus.UNAVAILABLE);
        assertThat(new CampaignObservationTarget(
                13L, CouponStatus.OPEN, false, SourceStatus.VALID, SNAPSHOT_AT).stockStatus())
                .isEqualTo(SourceStatus.VALID);
        assertThat(new CampaignObservationTarget(
                13L, CouponStatus.OPEN, false, SourceStatus.VALID, SNAPSHOT_AT).stockObservedAt())
                .isEqualTo(SNAPSHOT_AT);
        assertThat(new CampaignObservationTarget(
                14L, CouponStatus.SCHEDULED, null, SourceStatus.N_A, null).stockStatus())
                .isEqualTo(SourceStatus.N_A);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                15L, CouponStatus.OPEN, null, SourceStatus.VALID, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                16L, CouponStatus.CLOSED, true, SourceStatus.N_A, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                17L, CouponStatus.OPEN, true, SourceStatus.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                18L, CouponStatus.OPEN, null, SourceStatus.VALID, SNAPSHOT_AT))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                18L, CouponStatus.OPEN, true, SourceStatus.VALID, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CampaignObservationTarget(
                19L, CouponStatus.CLOSED, null, SourceStatus.PENDING, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 값 없는 OPEN 재고보다 O1 상태가 낫다고 위조할 수 없고 더 나쁜 원천 상태는 보존할 수 있습니다. */
    @Test
    void dataPreservesNonValueOpenStockStatusInO1Input() {
        OverviewObservationRequest pendingRequest = new OverviewObservationRequest(SNAPSHOT_AT,
                List.of(new CampaignObservationTarget(
                        20L, CouponStatus.OPEN, null, SourceStatus.PENDING)), POLICY);
        OverviewObservationRequest unavailableRequest = new OverviewObservationRequest(SNAPSHOT_AT,
                List.of(new CampaignObservationTarget(
                        21L, CouponStatus.OPEN, null, SourceStatus.UNAVAILABLE)), POLICY);

        assertThatThrownBy(() -> data(pendingRequest, List.of(flowInput(20L, 1L, 1L)),
                SourceStatus.PENDING, SourceStatus.PENDING))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(data(pendingRequest, List.of(missingFlowInput(20L, SourceStatus.PENDING)),
                SourceStatus.PENDING, SourceStatus.PENDING).issuanceFlowInputs().getFirst().sourceStatus())
                .isEqualTo(SourceStatus.PENDING);
        assertThat(data(pendingRequest, List.of(missingFlowInput(20L, SourceStatus.UNAVAILABLE)),
                SourceStatus.PENDING, SourceStatus.PENDING).issuanceFlowInputs().getFirst().sourceStatus())
                .isEqualTo(SourceStatus.UNAVAILABLE);
        assertThatThrownBy(() -> data(pendingRequest, List.of(missingFlowInput(20L, SourceStatus.N_A)),
                SourceStatus.PENDING, SourceStatus.PENDING))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(unavailableRequest,
                List.of(missingFlowInput(21L, SourceStatus.PENDING)),
                SourceStatus.PENDING, SourceStatus.PENDING))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 값 있는 OPEN 재고의 최신성보다 O1 응답 상태가 좋아지는 것을 기술 중립 경계에서 거부합니다. */
    @Test
    void dataRejectsO1StatusThatPromotesValueCarryingStockSource() {
        OverviewObservationRequest staleRequest = new OverviewObservationRequest(SNAPSHOT_AT,
                List.of(new CampaignObservationTarget(
                        22L, CouponStatus.OPEN, true, SourceStatus.STALE,
                        SNAPSHOT_AT.minusSeconds(1))), POLICY);
        OverviewObservationRequest warmingRequest = new OverviewObservationRequest(SNAPSHOT_AT,
                List.of(new CampaignObservationTarget(
                        23L, CouponStatus.OPEN, true, SourceStatus.WARMING_UP,
                        SNAPSHOT_AT.minusSeconds(1))), POLICY);

        assertThatThrownBy(() -> data(staleRequest, List.of(flowInput(22L, 1L, 1L)),
                SourceStatus.PENDING, SourceStatus.PENDING))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(warmingRequest, List.of(flowInput(23L, 1L, 1L)),
                SourceStatus.PENDING, SourceStatus.PENDING))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 데이터는 O1 원본 목록을 방어 복사하고 독립 모집단의 성공·시도 count를 보존합니다. */
    @Test
    void dataDefensivelyCopiesO1InputsAndAcceptsIndependentAttemptAndSuccessPopulations() {
        OverviewObservationRequest request = request(21L);
        List<IssuanceFlowCalculator.IssuanceFlowInput> inputs = new ArrayList<>();
        inputs.add(flowInput(21L, 0L, 1L));

        OverviewObservationData data = data(request, inputs, SourceStatus.VALID, SourceStatus.NO_TRAFFIC);
        inputs.clear();

        assertThat(data.issuanceFlowInputs()).extracting(IssuanceFlowCalculator.IssuanceFlowInput::couponId)
                .containsExactly(21L);
        assertThat(data.issuanceFlowInputs().getFirst().attemptedCount()).isZero();
        assertThat(data.issuanceFlowInputs().getFirst().completedCount()).isEqualTo(1d);
        assertThatThrownBy(() -> data.issuanceFlowInputs().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** O1 데이터는 요청한 캠페인 모집단을 빠짐없이 중복 없이 정확히 한 번씩만 포함해야 합니다. */
    @Test
    void dataRejectsMissingExtraAndDuplicateO1CampaignInputs() {
        OverviewObservationRequest request = request(31L, 32L);

        assertThatThrownBy(() -> data(request, List.of(flowInput(31L, 1L, 1L)),
                SourceStatus.VALID, SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(request, List.of(flowInput(31L, 1L, 1L), flowInput(32L, 1L, 1L),
                flowInput(33L, 1L, 1L)), SourceStatus.VALID, SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(request, List.of(flowInput(31L, 1L, 1L), flowInput(31L, 2L, 2L),
                flowInput(32L, 1L, 1L)), SourceStatus.VALID, SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** O1 입력은 같은 couponId라도 요청 대상의 캠페인 상태와 진행 중 재고 조건을 바꿀 수 없습니다. */
    @Test
    void dataRejectsO1InputThatContradictsRequestedCampaignStateOrOpenStock() {
        OverviewObservationRequest openRequest = request(37L);
        OverviewObservationRequest closedRequest = new OverviewObservationRequest(SNAPSHOT_AT,
                List.of(new CampaignObservationTarget(
                        38L, CouponStatus.CLOSED, null, SourceStatus.N_A, null)), POLICY);

        assertThatThrownBy(() -> data(openRequest, List.of(flowInput(
                37L, CouponStatus.CLOSED, false, 1L, 1L, SNAPSHOT_AT)),
                SourceStatus.VALID, SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(openRequest, List.of(flowInput(
                37L, CouponStatus.OPEN, false, 1L, 1L, SNAPSHOT_AT)),
                SourceStatus.VALID, SourceStatus.VALID))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(data(closedRequest, List.of(flowInput(
                38L, CouponStatus.CLOSED, false, 1L, 1L, SNAPSHOT_AT)),
                SourceStatus.VALID, SourceStatus.VALID).issuanceFlowInputs())
                .extracting(IssuanceFlowCalculator.IssuanceFlowInput::couponId)
                .containsExactly(38L);
    }

    /** 요청 기준 시각 뒤의 O1·O3·전체 관측은 하나라도 포함되면 거부해야 합니다. */
    @Test
    void dataRejectsValueCarryingObservationsAfterSnapshotAt() {
        OverviewObservationRequest request = request(61L);
        Instant future = SNAPSHOT_AT.plusSeconds(1);
        List<IssuanceFlowCalculator.IssuanceFlowInput> inputs = List.of(flowInput(61L, 1L, 1L));
        CustomerOutcomeCalculator.OutcomeInput outcome = outcomeInput();
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateIssuanceRate> issuanceRate =
                aggregateIssuanceRate(SourceStatus.VALID);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latency =
                latencySummary(SourceStatus.VALID);

        assertThatThrownBy(() -> data(request, List.of(flowInput(
                61L, CouponStatus.OPEN, true, 1L, 1L, future)), outcome, issuanceRate, latency))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(request, inputs, outcomeInput(future), issuanceRate, latency))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(request, inputs, outcome, aggregateIssuanceRate(future), latency))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> data(request, inputs, outcome, issuanceRate, latencySummary(future)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Snapshot보다 이른 STALE 관측은 값과 관측 시각을 유지한 유효한 과거 원천입니다. */
    @Test
    void dataAcceptsEarlierStaleObservations() {
        Instant observedAt = SNAPSHOT_AT.minusSeconds(1);
        Instant windowStart = observedAt.minus(Duration.ofMinutes(1));
        OverviewObservationData data = data(request(71L), List.of(staleFlowInput(71L, windowStart, observedAt)),
                new CustomerOutcomeCalculator.OutcomeInput(windowStart, observedAt, List.of(),
                        SourceStatus.STALE, observedAt),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.AggregateIssuanceRate(2.0, 3.0), SourceStatus.STALE, observedAt),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(Duration.ofMillis(10), null,
                                windowStart, observedAt), SourceStatus.STALE, observedAt));

        assertThat(data.issuanceFlowInputs().getFirst().observedAt()).isEqualTo(observedAt);
        assertThat(data.outcomeInput().observedAt()).isEqualTo(observedAt);
        assertThat(data.aggregateIssuanceRate().observedAt()).isEqualTo(observedAt);
        assertThat(data.latencySummary().observedAt()).isEqualTo(observedAt);
    }

    /** 평가 구간과 원시 scrape 시각은 서로 다른 축이며 둘 다 snapshot 경계 안이면 유효합니다. */
    @Test
    void dataAcceptsSnapshotEvaluationWindowsAfterRawScrapeTime() {
        Instant observedAt = SNAPSHOT_AT.minusSeconds(17);
        IssuanceFlowCalculator.IssuanceFlowInput flow = new IssuanceFlowCalculator.IssuanceFlowInput(
                72L, CouponStatus.OPEN, true, WINDOW_START, SNAPSHOT_AT,
                SNAPSHOT_AT.minus(Duration.ofMinutes(10)), SNAPSHOT_AT,
                1d, 0d, 1d, SNAPSHOT_AT.minus(Duration.ofMinutes(2)), WINDOW_START,
                List.of(), null, WINDOW_START, SourceStatus.VALID, observedAt);
        CustomerOutcomeCalculator.OutcomeInput outcome = new CustomerOutcomeCalculator.OutcomeInput(
                SNAPSHOT_AT.minus(Duration.ofMinutes(5)), SNAPSHOT_AT, List.of(),
                SourceStatus.VALID, observedAt);
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latency =
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(Duration.ofMillis(10), null,
                                SNAPSHOT_AT.minusSeconds(10), SNAPSHOT_AT),
                        SourceStatus.VALID, observedAt);

        OverviewObservationData data = data(request(72L), List.of(flow), outcome,
                aggregateIssuanceRate(SourceStatus.PENDING), latency);

        assertThat(data.issuanceFlowInputs().getFirst().windowEnd()).isEqualTo(SNAPSHOT_AT);
        assertThat(data.outcomeInput().windowEnd()).isEqualTo(SNAPSHOT_AT);
        assertThat(data.latencySummary().value().windowEnd()).isEqualTo(SNAPSHOT_AT);
        assertThat(data.latencySummary().observedAt()).isEqualTo(observedAt);
    }

    /** 값 있는 모든 평가 구간은 raw scrape 시각과 무관하게 request snapshot을 넘을 수 없습니다. */
    @Test
    void dataRejectsEvaluationWindowsAfterRequestSnapshot() {
        Instant future = SNAPSHOT_AT.plusSeconds(1);

        assertThatThrownBy(() -> data(request(73L), List.of(
                new IssuanceFlowCalculator.IssuanceFlowInput(
                        73L, CouponStatus.OPEN, true, WINDOW_START, future,
                        SNAPSHOT_AT.minus(Duration.ofMinutes(10)), SNAPSHOT_AT,
                        1d, 0d, 1d, SNAPSHOT_AT.minus(Duration.ofMinutes(2)), WINDOW_START,
                        List.of(), null, WINDOW_START, SourceStatus.VALID, SNAPSHOT_AT)),
                outcomeInput(), aggregateIssuanceRate(SourceStatus.PENDING),
                latencySummary(SourceStatus.PENDING)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshotAt");
        assertThatThrownBy(() -> data(request(74L), List.of(flowInput(74L, 1L, 1L)),
                new CustomerOutcomeCalculator.OutcomeInput(
                        WINDOW_START, future, List.of(), SourceStatus.VALID, SNAPSHOT_AT),
                aggregateIssuanceRate(SourceStatus.PENDING), latencySummary(SourceStatus.PENDING)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshotAt");
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> futureLatency =
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(Duration.ofMillis(10), null,
                                WINDOW_START, future), SourceStatus.VALID, SNAPSHOT_AT);
        assertThatThrownBy(() -> data(request(75L), List.of(flowInput(75L, 1L, 1L)),
                outcomeInput(), aggregateIssuanceRate(SourceStatus.PENDING), futureLatency))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshotAt");
    }

    /** 지연 외부 관측 시각이 기준 시각이어도 포함한 관측 구간은 미래로 끝날 수 없습니다. */
    @Test
    void dataRejectsLatencyWindowEndingAfterItsObservedAt() {
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> futureLatencyWindow =
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(Duration.ofMillis(10), null,
                                WINDOW_START, SNAPSHOT_AT.plusSeconds(1)), SourceStatus.VALID, SNAPSHOT_AT);

        assertThatThrownBy(() -> data(request(75), List.of(flowInput(75L, 1L, 1L)), outcomeInput(),
                aggregateIssuanceRate(SourceStatus.VALID), futureLatencyWindow))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 값 없는 O1 입력도 원본과 노출 목록 어느 쪽에서도 버킷을 변경할 수 없습니다. */
    @Test
    void dataKeepsNonValueO1BucketsImmutable() {
        IssuanceFlowCalculator.IssuanceBucket bucket = new IssuanceFlowCalculator.IssuanceBucket(
                WINDOW_START, SNAPSHOT_AT, 0L);
        List<IssuanceFlowCalculator.IssuanceBucket> buckets = new ArrayList<>();
        buckets.add(bucket);
        OverviewObservationData data = data(request(81L), List.of(pendingFlowInput(81L, buckets)),
                SourceStatus.PENDING, SourceStatus.PENDING);

        buckets.clear();

        assertThat(data.issuanceFlowInputs().getFirst().buckets()).containsExactly(bucket);
        assertThatThrownBy(() -> data.issuanceFlowInputs().getFirst().buckets().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 값 없는 O3 입력도 원본과 노출 목록 어느 쪽에서도 결과 count를 변경할 수 없습니다. */
    @Test
    void dataKeepsNonValueO3CountsImmutable() {
        CustomerOutcomeCalculator.OutcomeCount count = new CustomerOutcomeCalculator.OutcomeCount(
                AdminOverviewSnapshot.CustomerOutcomeType.ISSUED, 0L);
        List<CustomerOutcomeCalculator.OutcomeCount> counts = new ArrayList<>();
        counts.add(count);
        CustomerOutcomeCalculator.OutcomeInput outcome = new CustomerOutcomeCalculator.OutcomeInput(
                null, null, counts, SourceStatus.PENDING, null);
        OverviewObservationData data = data(request(82L), List.of(flowInput(82L, 1L, 1L)), outcome,
                aggregateIssuanceRate(SourceStatus.PENDING), latencySummary(SourceStatus.PENDING));

        counts.clear();

        assertThat(data.outcomeInput().counts()).containsExactly(count);
        assertThatThrownBy(() -> data.outcomeInput().counts().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /** 값 없는 상태는 값·관측 시각 없이, 값 있는 상태는 기존 Observation 규칙을 그대로 보존합니다. */
    @Test
    void dataPreservesObservationValueAndTimeRulesThroughExistingObservationConstructor() {
        OverviewObservationData pending = data(request(41L), List.of(flowInput(41L, 1L, 1L)),
                SourceStatus.PENDING, SourceStatus.UNAVAILABLE);
        OverviewObservationData valued = data(request(42L), List.of(flowInput(42L, 0L, 0L)),
                SourceStatus.VALID, SourceStatus.NO_TRAFFIC);

        assertThat(pending.aggregateIssuanceRate())
                .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, SourceStatus.PENDING, null));
        assertThat(pending.latencySummary())
                .isEqualTo(new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null));
        assertThat(valued.aggregateIssuanceRate().value())
                .isEqualTo(new AdminOverviewSnapshot.AggregateIssuanceRate(2.0, 3.0));
        assertThat(valued.aggregateIssuanceRate().observedAt()).isEqualTo(SNAPSHOT_AT);
        assertThat(valued.latencySummary().value())
                .isEqualTo(new AdminOverviewSnapshot.LatencySummary(Duration.ofMillis(10), null,
                        WINDOW_START, SNAPSHOT_AT));
        assertThat(valued.latencySummary().observedAt()).isEqualTo(SNAPSHOT_AT);
    }

    /** 관측 원천은 Spring 또는 API 의존성 없이 메모리 람다로 계약을 구현할 수 있습니다. */
    @Test
    void sourceCanBeImplementedByInMemoryLambda() {
        OverviewObservationRequest request = request(51L);
        OverviewObservationSource source = observationRequest -> data(observationRequest,
                List.of(flowInput(51L, 1L, 1L)), SourceStatus.VALID, SourceStatus.VALID);

        OverviewObservationData result = source.observe(request);

        assertThat(result.request()).isEqualTo(request);
        assertThat(result.outcomeInput().sourceStatus()).isEqualTo(SourceStatus.VALID);
    }

    /** 유효한 진행 캠페인 관측 대상을 만듭니다. */
    private static CampaignObservationTarget target(long couponId) {
        return new CampaignObservationTarget(
                couponId, CouponStatus.OPEN, true, SourceStatus.VALID, SNAPSHOT_AT);
    }

    /** 지정 couponId의 요청 모집단을 만듭니다. */
    private static OverviewObservationRequest request(long... couponIds) {
        List<CampaignObservationTarget> targets = new ArrayList<>();
        for (long couponId : couponIds) {
            targets.add(target(couponId));
        }
        return new OverviewObservationRequest(SNAPSHOT_AT, targets, POLICY);
    }

    /** 독립 시도·성공 모집단을 갖는 값 있는 O1 입력을 만듭니다. */
    private static IssuanceFlowCalculator.IssuanceFlowInput flowInput(
            long couponId, double attemptedCount, double completedCount
    ) {
        return flowInput(couponId, CouponStatus.OPEN, true, attemptedCount, completedCount, SNAPSHOT_AT);
    }

    /** 지정 상태·재고·관측 시각을 갖는 값 있는 O1 입력을 만듭니다. */
    private static IssuanceFlowCalculator.IssuanceFlowInput flowInput(
            long couponId,
            CouponStatus campaignStatus,
            boolean stockAvailable,
            double attemptedCount,
            double completedCount,
            Instant observedAt
    ) {
        return new IssuanceFlowCalculator.IssuanceFlowInput(
                couponId, campaignStatus, stockAvailable, WINDOW_START, SNAPSHOT_AT,
                WINDOW_START, SNAPSHOT_AT,
                attemptedCount, completedCount, completedCount,
                WINDOW_START, SNAPSHOT_AT, List.of(), completedCount == 0d ? null : SNAPSHOT_AT,
                WINDOW_START, SourceStatus.VALID, observedAt);
    }

    /** 지정 과거 구간과 관측 시각을 갖는 값 있는 STALE O1 입력을 만듭니다. */
    private static IssuanceFlowCalculator.IssuanceFlowInput staleFlowInput(
            long couponId,
            Instant windowStart,
            Instant observedAt
    ) {
        return new IssuanceFlowCalculator.IssuanceFlowInput(
                couponId, CouponStatus.OPEN, true, windowStart, observedAt,
                windowStart, observedAt,
                1d, 1d, 1d, windowStart, observedAt, List.of(), observedAt,
                windowStart, SourceStatus.STALE, observedAt);
    }

    /** 지정 버킷 목록을 보존하는 값 없는 PENDING O1 입력을 만듭니다. */
    private static IssuanceFlowCalculator.IssuanceFlowInput pendingFlowInput(
            long couponId,
            List<IssuanceFlowCalculator.IssuanceBucket> buckets
    ) {
        return new IssuanceFlowCalculator.IssuanceFlowInput(
                couponId, CouponStatus.OPEN, null, null, null, null, null,
                null, null, null, null, null, buckets, null,
                null, SourceStatus.PENDING, null);
    }

    /** 지정 값 없는 상태를 갖는 OPEN O1 입력을 만듭니다. */
    private static IssuanceFlowCalculator.IssuanceFlowInput missingFlowInput(
            long couponId,
            SourceStatus sourceStatus
    ) {
        return new IssuanceFlowCalculator.IssuanceFlowInput(
                couponId, CouponStatus.OPEN, null, null, null, null, null,
                null, null, null, null, null, List.of(), null,
                null, sourceStatus, null);
    }

    /** 지정 상태의 전체 관측값과 O3 입력을 포함하는 관측 데이터를 만듭니다. */
    private static OverviewObservationData data(
            OverviewObservationRequest request,
            List<IssuanceFlowCalculator.IssuanceFlowInput> inputs,
            SourceStatus issuanceStatus,
            SourceStatus latencyStatus
    ) {
        return data(request, inputs, outcomeInput(), aggregateIssuanceRate(issuanceStatus), latencySummary(latencyStatus));
    }

    /** 지정 O1·O3·전체 관측값으로 관측 데이터 계약을 생성합니다. */
    private static OverviewObservationData data(
            OverviewObservationRequest request,
            List<IssuanceFlowCalculator.IssuanceFlowInput> inputs,
            CustomerOutcomeCalculator.OutcomeInput outcomeInput,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateIssuanceRate> issuanceRate,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latency
    ) {
        return new OverviewObservationData(request, inputs, outcomeInput, issuanceRate, latency);
    }

    /** 값 있는 O3 입력을 만듭니다. */
    private static CustomerOutcomeCalculator.OutcomeInput outcomeInput() {
        return new CustomerOutcomeCalculator.OutcomeInput(WINDOW_START, SNAPSHOT_AT, List.of(),
                SourceStatus.VALID, SNAPSHOT_AT);
    }

    /** 지정 관측 시각을 갖는 값 있는 O3 입력을 만듭니다. */
    private static CustomerOutcomeCalculator.OutcomeInput outcomeInput(Instant observedAt) {
        return new CustomerOutcomeCalculator.OutcomeInput(WINDOW_START, SNAPSHOT_AT, List.of(),
                SourceStatus.VALID, observedAt);
    }

    /** 지정 원천 상태의 전체 발급률 관측값을 만듭니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateIssuanceRate>
            aggregateIssuanceRate(SourceStatus status) {
        return status.carriesValue()
                ? new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.AggregateIssuanceRate(2.0, 3.0), status, SNAPSHOT_AT)
                : new AdminOverviewSnapshot.Observation<>(null, status, null);
    }

    /** 지정 관측 시각을 갖는 값 있는 전체 발급률 관측값을 만듭니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateIssuanceRate>
            aggregateIssuanceRate(Instant observedAt) {
        return new AdminOverviewSnapshot.Observation<>(
                new AdminOverviewSnapshot.AggregateIssuanceRate(2.0, 3.0), SourceStatus.VALID, observedAt);
    }

    /** 지정 원천 상태의 지연 관측값을 만듭니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latencySummary(
            SourceStatus status
    ) {
        return status.carriesValue()
                ? new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.LatencySummary(Duration.ofMillis(10), null,
                                WINDOW_START, SNAPSHOT_AT), status, SNAPSHOT_AT)
                : new AdminOverviewSnapshot.Observation<>(null, status, null);
    }

    /** 지정 관측 시각을 갖는 값 있는 지연 관측값을 만듭니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latencySummary(
            Instant observedAt
    ) {
        return new AdminOverviewSnapshot.Observation<>(
                new AdminOverviewSnapshot.LatencySummary(Duration.ofMillis(10), null,
                        WINDOW_START, SNAPSHOT_AT), SourceStatus.VALID, observedAt);
    }
}

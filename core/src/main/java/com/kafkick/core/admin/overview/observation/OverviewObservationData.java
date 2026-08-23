package com.kafkick.core.admin.overview.observation;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot.AggregateIssuanceRate;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.LatencySummary;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.Observation;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeInput;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

/**
 * 관리자 운영현황 계산기로 전달할 O1·O3·전체 관측의 기술 중립 묶음입니다.
 *
 * <p>O1 입력은 요청한 각 캠페인을 정확히 한 번씩 포함하고, 같은 캠페인 상태를 유지해야 합니다. 진행
 * 캠페인의 값 있는 O1 입력은 대상의 명시 재고 가능 여부도 유지합니다. 진행 중이 아닌 대상의 재고는
 * O1 상태 판정에 의미가 없으므로 기존 입력이 가진 값을 정규화하거나 비교하지 않습니다. 전체 발급률과
 * 지연 관측은 기존 {@link Observation} 생성자가 보장하는 상태·값·관측 시각 규칙을 그대로 사용합니다.</p>
 *
 * @param request 이 관측 묶음이 응답하는 기준 시각과 캠페인 모집단
 * @param issuanceFlowInputs 요청 캠페인마다 하나씩 존재하는 O1 입력
 * @param outcomeInput 전체 캠페인 O3 고객 결과 입력
 * @param aggregateIssuanceRate 전체 캠페인 발급률 관측
 * @param latencySummary 성공·실패 응답 지연 관측
 */
public record OverviewObservationData(
        OverviewObservationRequest request,
        List<IssuanceFlowInput> issuanceFlowInputs,
        OutcomeInput outcomeInput,
        Observation<AggregateIssuanceRate> aggregateIssuanceRate,
        Observation<LatencySummary> latencySummary
) {

    /** 모든 관측 입력을 검증하고 O1 목록을 불변 복사하며 요청 모집단과 일치시킵니다. */
    public OverviewObservationData {
        Objects.requireNonNull(request, "request");
        issuanceFlowInputs = List.copyOf(Objects.requireNonNull(issuanceFlowInputs, "issuanceFlowInputs"));
        Objects.requireNonNull(outcomeInput, "outcomeInput");
        Objects.requireNonNull(aggregateIssuanceRate, "aggregateIssuanceRate");
        Objects.requireNonNull(latencySummary, "latencySummary");
        validateCampaignPopulation(request, issuanceFlowInputs);
        validateSnapshotBoundary(request, issuanceFlowInputs, outcomeInput, aggregateIssuanceRate, latencySummary);
    }

    /** 요청 대상과 O1 입력이 같은 쿠폰 ID·캠페인 상태 모집단을 중복 없이 표현하는지 검증합니다. */
    private static void validateCampaignPopulation(
            OverviewObservationRequest request,
            List<IssuanceFlowInput> issuanceFlowInputs
    ) {
        Map<Long, CampaignObservationTarget> targetByCouponId = new HashMap<>();
        for (CampaignObservationTarget campaignTarget : request.campaignTargets()) {
            targetByCouponId.put(campaignTarget.couponId(), campaignTarget);
        }
        Set<Long> observedCouponIds = new HashSet<>();
        for (IssuanceFlowInput issuanceFlowInput : issuanceFlowInputs) {
            Objects.requireNonNull(issuanceFlowInput, "issuanceFlowInputs에는 null을 포함할 수 없습니다.");
            if (!observedCouponIds.add(issuanceFlowInput.couponId())) {
                throw new IllegalArgumentException("issuanceFlowInputs의 couponId는 중복될 수 없습니다.");
            }
            CampaignObservationTarget campaignTarget = targetByCouponId.get(issuanceFlowInput.couponId());
            if (campaignTarget != null) {
                validateTargetMetadata(campaignTarget, issuanceFlowInput);
            }
        }
        // 빠진 대상과 요청에 없던 입력을 모두 같은 모집단 불일치로 거부합니다.
        if (!targetByCouponId.keySet().equals(observedCouponIds)) {
            throw new IllegalArgumentException("O1 캠페인 모집단이 요청 대상과 일치해야 합니다.");
        }
    }

    /** 요청 대상의 캠페인 상태와 진행 중 재고 가능 여부가 O1 입력과 모순되지 않는지 검증합니다. */
    private static void validateTargetMetadata(
            CampaignObservationTarget campaignTarget,
            IssuanceFlowInput issuanceFlowInput
    ) {
        if (campaignTarget.campaignStatus() != issuanceFlowInput.campaignStatus()) {
            throw new IllegalArgumentException("O1 입력의 campaignStatus가 요청 대상과 일치해야 합니다.");
        }
        // 값 없는 OPEN 입력은 기존 O1 계약에 따라 stockAvailable이 null이므로 원천 미관측을 보존합니다.
        if (campaignTarget.campaignStatus() == CouponStatus.OPEN
                && issuanceFlowInput.sourceStatus().carriesValue()
                && !campaignTarget.stockAvailable().equals(issuanceFlowInput.stockAvailable())) {
            throw new IllegalArgumentException("진행 중 O1 입력의 stockAvailable이 요청 대상과 일치해야 합니다.");
        }
    }

    /** 값 있는 모든 관측이 요청한 Snapshot 기준 시각 이후의 원천을 포함하지 않는지 검증합니다. */
    private static void validateSnapshotBoundary(
            OverviewObservationRequest request,
            List<IssuanceFlowInput> issuanceFlowInputs,
            OutcomeInput outcomeInput,
            Observation<AggregateIssuanceRate> aggregateIssuanceRate,
            Observation<LatencySummary> latencySummary
    ) {
        for (IssuanceFlowInput issuanceFlowInput : issuanceFlowInputs) {
            validateObservedAtNoLaterThan(request.snapshotAt(), issuanceFlowInput.sourceStatus(),
                    issuanceFlowInput.observedAt(), "O1");
            validateFlowEvaluationBoundary(request.snapshotAt(), issuanceFlowInput);
        }
        validateObservedAtNoLaterThan(request.snapshotAt(), outcomeInput.sourceStatus(),
                outcomeInput.observedAt(), "O3");
        if (outcomeInput.sourceStatus().carriesValue()) {
            validateEvaluationEnd(request.snapshotAt(), outcomeInput.windowEnd(), "O3 관측 구간");
        }
        validateObservedAtNoLaterThan(request.snapshotAt(), aggregateIssuanceRate.status(),
                aggregateIssuanceRate.observedAt(), "전체 발급률");
        validateObservedAtNoLaterThan(request.snapshotAt(), latencySummary.status(),
                latencySummary.observedAt(), "지연");
        validateLatencyWindow(request.snapshotAt(), latencySummary);
    }

    /** O1의 모든 평가 구간·이벤트 시각이 요청 snapshot을 넘지 않는지 검증합니다. */
    private static void validateFlowEvaluationBoundary(
            Instant snapshotAt,
            IssuanceFlowInput input
    ) {
        if (!input.sourceStatus().carriesValue()) {
            return;
        }
        validateEvaluationEnd(snapshotAt, input.windowEnd(), "O1 현재 구간");
        validateEvaluationEnd(snapshotAt, input.trendWindowEnd(), "O1 추세 구간");
        validateEvaluationEnd(snapshotAt, input.comparisonWindowEnd(), "O1 비교 구간");
        validateEvaluationEnd(snapshotAt, input.conditionStartedAt(), "O1 조건 시작 시각");
        if (input.lastCompletedAt() != null) {
            validateEvaluationEnd(snapshotAt, input.lastCompletedAt(), "O1 마지막 완료 시각");
        }
        for (com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceBucket bucket
                : input.buckets()) {
            validateEvaluationEnd(snapshotAt, bucket.windowEnd(), "O1 추세 버킷");
        }
    }

    /** 값 있는 지연 관측의 구간이 양수이고 요청 snapshot 이후를 포함하지 않는지 검증합니다. */
    private static void validateLatencyWindow(
            Instant snapshotAt,
            Observation<LatencySummary> latencyObservation
    ) {
        if (!latencyObservation.status().carriesValue()) {
            return;
        }
        LatencySummary latencySummary = latencyObservation.value();
        if (latencySummary.windowStart() == null || latencySummary.windowEnd() == null
                || !latencySummary.windowEnd().isAfter(latencySummary.windowStart())) {
            throw new IllegalArgumentException("지연 관측 구간은 양수여야 합니다.");
        }
        validateEvaluationEnd(snapshotAt, latencySummary.windowEnd(), "지연 관측 구간");
    }

    /** 평가 구간 종료·이벤트 시각이 요청 snapshot 이후이면 거부합니다. */
    private static void validateEvaluationEnd(Instant snapshotAt, Instant end, String name) {
        if (end.isAfter(snapshotAt)) {
            throw new IllegalArgumentException(name + "은 snapshotAt 이후일 수 없습니다.");
        }
    }

    /** 값 있는 원천 관측 시각이 요청 기준 시각보다 뒤인지 확인합니다. */
    private static void validateObservedAtNoLaterThan(
            Instant snapshotAt,
            SourceStatus sourceStatus,
            Instant observedAt,
            String observationName
    ) {
        if (sourceStatus.carriesValue() && observedAt.isAfter(snapshotAt)) {
            throw new IllegalArgumentException(observationName + " 관측 시각은 snapshotAt 이후일 수 없습니다.");
        }
    }
}

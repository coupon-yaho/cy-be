package com.kafkick.core.admin.overview.calculator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** O2 대기 변화·입장 처리율·ETA와 전체 대기 위험을 계산하는 순수 계산기입니다. */
@Component
public class CampaignQueueCalculator {

    /** 상태 없는 O2 순수 계산기를 생성합니다. */
    public CampaignQueueCalculator() { }

    /**
     * 캠페인별 대기열과 전체 KPI를 동일한 캠페인 계산 결과에서 만듭니다.
     *
     * <p>입장률 0은 실제 처리량 0이며 ETA는 null입니다. 적용 캠페인 하나라도 수집 불가면
     * 부분 합계를 전체값처럼 내보내지 않고 두 전체 관측값을 UNAVAILABLE로 반환합니다.</p>
     *
     * @param policy 안내·입장 중단 정책
     * @param inputs 쿠폰별 대기열 원천 입력
     * @return 캠페인 행, 전역 KPI 및 QUEUE_STALLED 후보를 함께 담은 불변 결과
     */
    public QueueCalculation calculate(OverviewCalculationPolicy policy, List<QueueInput> inputs) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(inputs, "inputs");
        Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus>> statuses =
                new LinkedHashMap<>();
        List<AdminOverviewSnapshot.OperationActionItem> actions = new ArrayList<>();
        List<SourceStatus> applicableStatuses = new ArrayList<>();
        long waitingTotal = 0L;
        double admissionsPerMinuteTotal = 0.0;
        Instant aggregateObservedAt = null;
        long riskCount = 0L;
        Duration longestWait = null;
        boolean hasUndeterminedWait = false;
        // 입력 조회 순서와 무관하게 합계와 후보 생성 순서를 couponId 기준으로 고정합니다.
        List<QueueInput> orderedInputs = inputs.stream()
                .sorted(java.util.Comparator.comparing(QueueInput::couponId))
                .toList();
        for (QueueInput input : orderedInputs) {
            Objects.requireNonNull(input, "inputs에는 null을 포함할 수 없습니다.");
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus> observation =
                    calculateOne(policy, input);
            if (statuses.put(input.couponId(), observation) != null) {
                throw new IllegalArgumentException("couponId는 중복될 수 없습니다.");
            }
            if (input.sourceStatus() == SourceStatus.N_A) {
                continue;
            }
            applicableStatuses.add(input.sourceStatus());
            // 값 없는 적용 대상은 행 상태에는 남기되 전역 숫자 부분 합계에서는 제외합니다.
            if (!input.sourceStatus().carriesValue()) {
                continue;
            }
            AdminOverviewSnapshot.CampaignQueueStatus queue = observation.value();
            waitingTotal = Math.addExact(waitingTotal, queue.waitingCount());
            admissionsPerMinuteTotal += queue.admissionsPerMinute();
            aggregateObservedAt = earlier(aggregateObservedAt, input.observedAt());
            if (queue.assessment() != AdminOverviewSnapshot.CampaignQueueAssessment.NORMAL) {
                riskCount++;
            }
            if (queue.waitingCount() > 0L && queue.estimatedWait() == null) {
                // 대기자는 있지만 처리율이 0이면 ETA를 0으로 위조하지 않고 계산 불가로 표시합니다.
                hasUndeterminedWait = true;
            }
            if (queue.estimatedWait() != null && (longestWait == null
                    || queue.estimatedWait().compareTo(longestWait) > 0)) {
                longestWait = queue.estimatedWait();
            }
            if (input.sourceStatus() == SourceStatus.VALID
                    && queue.assessment() == AdminOverviewSnapshot.CampaignQueueAssessment.ADMISSION_STOPPED) {
                // 오래되거나 준비 중인 판정은 조치 후보로 승격하지 않고 최신 정상 관측만 사용합니다.
                actions.add(stalledAction(input));
            }
        }
        SourceStatus aggregateStatus = aggregateStatus(applicableStatuses);
        // 적용 캠페인 하나라도 값 없는 상태면 계산 가능한 일부를 전체 KPI처럼 반환하지 않습니다.
        if (!aggregateStatus.carriesValue()) {
            return new QueueCalculation(statuses,
                    unavailableObservation(aggregateStatus), unavailableObservation(aggregateStatus), actions);
        }
        Duration aggregateEta = hasUndeterminedWait || admissionsPerMinuteTotal == 0.0 ? null
                : secondsCeiling(waitingTotal / admissionsPerMinuteTotal * 60.0);
        return new QueueCalculation(statuses,
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.QueueRiskSummary(
                                riskCount, hasUndeterminedWait ? null : longestWait),
                        aggregateStatus, aggregateObservedAt),
                new AdminOverviewSnapshot.Observation<>(
                        new AdminOverviewSnapshot.AggregateQueue(waitingTotal, admissionsPerMinuteTotal / 60.0,
                                aggregateEta), aggregateStatus, aggregateObservedAt), actions);
    }

    /** 한 캠페인의 실제 구간 입장 수를 분당 처리율과 ETA로 변환합니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus> calculateOne(
            OverviewCalculationPolicy policy, QueueInput input) {
        if (!input.sourceStatus().carriesValue()) {
            return unavailableObservation(input.sourceStatus());
        }
        requireWindow(input.windowStart(), input.windowEnd());
        // 실제 관측 구간의 입장 건수를 분당 처리율로 환산해 구간 길이 차이를 제거합니다.
        double admissionsPerMinute = input.admittedCount() * 60.0
                / Duration.between(input.windowStart(), input.windowEnd()).toNanos() * 1_000_000_000.0;
        Duration eta = admissionsPerMinute == 0.0 ? null
                : secondsCeiling(input.currentWaitingCount() / admissionsPerMinute * 60.0);
        AdminOverviewSnapshot.CampaignQueueAssessment assessment = assessmentOf(policy, input, eta);
        return new AdminOverviewSnapshot.Observation<>(new AdminOverviewSnapshot.CampaignQueueStatus(
                input.currentWaitingCount(), trend(input.currentWaitingCount() - input.previousWaitingCount()),
                input.currentWaitingCount() - input.previousWaitingCount(), admissionsPerMinute, eta, assessment),
                input.sourceStatus(), input.observedAt());
    }

    /** 중단 판정은 안내 기준 초과보다 우선하며 대기자가 0이면 중단이 아닙니다. */
    private static AdminOverviewSnapshot.CampaignQueueAssessment assessmentOf(
            OverviewCalculationPolicy policy, QueueInput input, Duration eta) {
        if (input.currentWaitingCount() > 0L && input.admittedCount() == 0L
                && input.admissionStoppedStartedAt() != null
                && !Duration.between(input.admissionStoppedStartedAt(), input.observedAt()).isNegative()
                && Duration.between(input.admissionStoppedStartedAt(), input.observedAt())
                .compareTo(policy.queueAdmissionStoppedAfter()) >= 0) {
            return AdminOverviewSnapshot.CampaignQueueAssessment.ADMISSION_STOPPED;
        }
        if (eta != null && eta.compareTo(policy.queueGuidanceThreshold()) > 0) {
            return AdminOverviewSnapshot.CampaignQueueAssessment.GUIDANCE_THRESHOLD_EXCEEDED;
        }
        return AdminOverviewSnapshot.CampaignQueueAssessment.NORMAL;
    }

    /** 대기 인원 차이의 부호만으로 입력 순서와 무관한 추세를 정합니다. */
    private static AdminOverviewSnapshot.TrendDirection trend(long delta) {
        if (delta > 0L) {
            return AdminOverviewSnapshot.TrendDirection.INCREASING;
        }
        if (delta < 0L) {
            return AdminOverviewSnapshot.TrendDirection.DECREASING;
        }
        return AdminOverviewSnapshot.TrendDirection.UNCHANGED;
    }

    /** 고객에게 짧은 시간을 약속하지 않도록 소수 초를 올림합니다. */
    private static Duration secondsCeiling(double seconds) {
        if (!Double.isFinite(seconds) || seconds < 0.0 || seconds >= 0x1.0p63) {
            throw new IllegalArgumentException("ETA 초 값이 유한한 Duration 범위를 벗어났습니다.");
        }
        return Duration.ofSeconds((long) Math.ceil(seconds));
    }

    /** 입력 구간의 0초·역전을 명시적 계약 오류로 처리합니다. */
    private static void requireWindow(Instant start, Instant end) {
        if (!end.isAfter(start)) {
            throw new IllegalArgumentException("관측 구간은 양수여야 합니다.");
        }
    }

    /** 구성값이 모두 유효한 시점을 보장하도록 가장 오래된 실제 관측 시각을 선택합니다. */
    private static Instant earlier(Instant left, Instant right) {
        return left == null || right.isBefore(left) ? right : left;
    }

    /** 적용 모집단의 N_A 제외 후 PENDING·UNAVAILABLE·STALE·WARMING_UP을 숨기지 않고 합성합니다. */
    private static SourceStatus aggregateStatus(List<SourceStatus> statuses) {
        if (statuses.isEmpty()) {
            return SourceStatus.N_A;
        }
        if (statuses.contains(SourceStatus.UNAVAILABLE)) {
            return SourceStatus.UNAVAILABLE;
        }
        if (statuses.contains(SourceStatus.PENDING)) {
            return SourceStatus.PENDING;
        }
        if (statuses.contains(SourceStatus.STALE)) {
            return SourceStatus.STALE;
        }
        if (statuses.contains(SourceStatus.WARMING_UP)) {
            return SourceStatus.WARMING_UP;
        }
        return statuses.stream().allMatch(status -> status == SourceStatus.NO_TRAFFIC)
                ? SourceStatus.NO_TRAFFIC : SourceStatus.VALID;
    }

    /** 값 없는 합성 상태는 부분 합계와 관측 시각 없이 그대로 표현합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> unavailableObservation(SourceStatus status) {
        return new AdminOverviewSnapshot.Observation<>(null, status, null);
    }

    /** 중단된 입장 흐름만 대표 조치 계산기에 넘길 후보를 만듭니다. */
    private static AdminOverviewSnapshot.OperationActionItem stalledAction(QueueInput input) {
        return new AdminOverviewSnapshot.OperationActionItem(input.couponId(), null, null, Severity.CRITICAL,
                AdminOverviewSnapshot.CustomerImpact.LIMITED, "대기열 입장 처리가 중단되었습니다.",
                input.admissionStoppedStartedAt(),
                Duration.between(input.admissionStoppedStartedAt(), input.observedAt()),
                new AdminOverviewSnapshot.RecommendedAction(AdminOverviewSnapshot.ActionCode.QUEUE_STALLED,
                        "대기열 입장 처리 확인", AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL));
    }

    /**
     * O2 원천 입력입니다.
     *
     * @param couponId 캠페인 식별자
     * @param currentWaitingCount 현재 실제 대기 인원; 0은 실제 빈 대기열
     * @param previousWaitingCount 정확히 1분 전 snapshot의 대기 인원; long 공개 계약의 분당 차이 기준
     * @param admittedCount 구간에서 실제 입장 완료된 수
     * @param windowStart 입장 완료 수 관측 시작 시각
     * @param windowEnd 입장 완료 수 관측 종료 시각
     * @param lastAdmissionAt 마지막 실제 입장 시각; 양수 admittedCount면 {@code [windowStart, windowEnd]}
     *                        폐구간 안에 필수이고, 0이면 해당 구간 안에 있을 수 없음
     * @param admissionStoppedStartedAt 대기자가 있는 연속 무입장 조건 시작 시각
     * @param sourceStatus 원천 상태
     * @param observedAt 값이 있는 상태의 실제 관측 시각
     */
    public record QueueInput(Long couponId, Long currentWaitingCount, Long previousWaitingCount,
                             Long admittedCount, Instant windowStart, Instant windowEnd,
                             Instant lastAdmissionAt, Instant admissionStoppedStartedAt, SourceStatus sourceStatus,
                             Instant observedAt) {
        /** 음수 인원과 상태·시각 불일치를 거부합니다. */
        public QueueInput {
            Objects.requireNonNull(couponId, "couponId");
            Objects.requireNonNull(sourceStatus, "sourceStatus");
            if (sourceStatus.carriesValue() != (observedAt != null)) {
                throw new IllegalArgumentException("원천 상태와 observedAt 조합이 맞지 않습니다.");
            }
            if (sourceStatus.carriesValue()) {
                Objects.requireNonNull(currentWaitingCount, "currentWaitingCount");
                Objects.requireNonNull(previousWaitingCount, "previousWaitingCount");
                Objects.requireNonNull(admittedCount, "admittedCount");
                Objects.requireNonNull(windowStart, "windowStart");
                Objects.requireNonNull(windowEnd, "windowEnd");
                if (currentWaitingCount < 0L || previousWaitingCount < 0L || admittedCount < 0L) {
                    throw new IllegalArgumentException("대기·입장 수는 음수일 수 없습니다.");
                }
                if (sourceStatus == SourceStatus.NO_TRAFFIC
                        && (currentWaitingCount != 0L || admittedCount != 0L)) {
                    throw new IllegalArgumentException("NO_TRAFFIC 대기·입장 count는 0이어야 합니다.");
                }
                requireWindow(windowStart, windowEnd);
                if (windowEnd.isAfter(observedAt)) {
                    throw new IllegalArgumentException("관측 구간 종료는 observedAt 이후일 수 없습니다.");
                }
                if (admittedCount > 0L && lastAdmissionAt == null) {
                    throw new IllegalArgumentException("입장이 있으면 lastAdmissionAt이 필요합니다.");
                }
                if (lastAdmissionAt != null && lastAdmissionAt.isAfter(observedAt)) {
                    throw new IllegalArgumentException("lastAdmissionAt은 observedAt 이후일 수 없습니다.");
                }
                if (admittedCount > 0L && (lastAdmissionAt.isBefore(windowStart)
                        || lastAdmissionAt.isAfter(windowEnd))) {
                    throw new IllegalArgumentException("입장이 있으면 lastAdmissionAt은 관측 구간 안이어야 합니다.");
                }
                if (admittedCount == 0L && lastAdmissionAt != null
                        && !lastAdmissionAt.isBefore(windowStart) && !lastAdmissionAt.isAfter(windowEnd)) {
                    throw new IllegalArgumentException("무입장 구간의 lastAdmissionAt은 관측 구간 안에 있을 수 없습니다.");
                }
                if (currentWaitingCount > 0L && admittedCount == 0L && admissionStoppedStartedAt == null) {
                    throw new IllegalArgumentException("대기 중 무입장은 admissionStoppedStartedAt이 필요합니다.");
                }
                if (admissionStoppedStartedAt != null && admissionStoppedStartedAt.isAfter(observedAt)) {
                    throw new IllegalArgumentException("admissionStoppedStartedAt은 observedAt 이후일 수 없습니다.");
                }
                if (admissionStoppedStartedAt != null) {
                    if (currentWaitingCount == 0L || admittedCount != 0L) {
                        throw new IllegalArgumentException("admissionStoppedStartedAt은 대기 중 무입장에서만 사용할 수 있습니다.");
                    }
                    if (lastAdmissionAt != null && lastAdmissionAt.isAfter(admissionStoppedStartedAt)) {
                        throw new IllegalArgumentException("lastAdmissionAt은 admissionStoppedStartedAt 이후일 수 없습니다.");
                    }
                }
            }
        }

    }

    /**
     * O2 계산 결과입니다.
     *
     * @param queueStatuses couponId별 행 관측값
     * @param queueRisk 전체 위험 KPI; 일부 미수집이면 UNAVAILABLE
     * @param aggregateQueue 전체 대기 KPI; 일부 미수집이면 UNAVAILABLE
     * @param actionCandidates 중단된 입장 흐름의 중복 없는 조치 후보
     */
    public record QueueCalculation(
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus>> queueStatuses,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.QueueRiskSummary> queueRisk,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateQueue> aggregateQueue,
            List<AdminOverviewSnapshot.OperationActionItem> actionCandidates) {
        /** 모든 결과 컬렉션을 불변 복사해 행·KPI·후보의 같은 계산 모집단을 보존합니다. */
        public QueueCalculation {
            queueStatuses = Map.copyOf(queueStatuses);
            Objects.requireNonNull(queueRisk, "queueRisk");
            Objects.requireNonNull(aggregateQueue, "aggregateQueue");
            actionCandidates = List.copyOf(actionCandidates);
        }
    }
}

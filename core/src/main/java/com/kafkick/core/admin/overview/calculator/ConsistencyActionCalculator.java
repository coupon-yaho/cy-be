package com.kafkick.core.admin.overview.calculator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.consistency.ConsistencyEvaluation;
import com.kafkick.core.consistency.ConsistencyFinalObservation;
import com.kafkick.core.consistency.ConsistencyGapType;
import com.kafkick.core.consistency.ConsistencyPhase;
import com.kafkick.core.consistency.Verdict;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/** FINAL 정합성 판정을 관리자 정합성 확인 조치 후보로 변환하는 순수 계산기입니다. */
@Component
public class ConsistencyActionCalculator {

    /** 캠페인별 FINAL 상태와 계산 가능한 조치 후보를 함께 보존한 일괄 계산 결과입니다. */
    public record FinalActionCalculation(
            List<AdminOverviewSnapshot.OperationActionItem> actionCandidates,
            Map<Long, AdminOverviewSnapshot.Observation<
                    List<AdminOverviewSnapshot.OperationActionItem>>> observations
    ) {
        public FinalActionCalculation {
            actionCandidates = List.copyOf(actionCandidates);
            observations = Collections.unmodifiableMap(new LinkedHashMap<>(observations));
        }

        /** FINAL 적용 모집단에 계산 대기나 사용할 수 없는 결과가 없는지 반환합니다. */
        public boolean isComplete() {
            return observations.values().stream()
                    .map(AdminOverviewSnapshot.Observation::status)
                    .noneMatch(status -> status == SourceStatus.PENDING
                            || status == SourceStatus.UNAVAILABLE);
        }
    }

    /** 상태 없는 FINAL 정합성 조치 후보 계산기를 생성합니다. */
    public ConsistencyActionCalculator() { }

    /**
     * FINAL 정합성 실패 또는 초과 발급을 최대 한 건의 조치 후보로 변환합니다.
     *
     * <p>LIVE 결과는 진행 중 추세이므로 조치로 승격하지 않습니다. FINAL에서 PENDING, UNAVAILABLE,
     * STALE 등 계산 불가 값, 엔진 버전과 맞지 않는 비적용 gap, 수치와 모순된 verdict·severity는
     * 정상 또는 빈 결과로 축약하지 않고 거부합니다.</p>
     *
     * @param context 캠페인 표시 정보와 FINAL 판정 확정 시각을 포함한 입력 문맥
     * @return 조치가 없거나 하나의 정합성 조치 후보
     * @throws NullPointerException 입력 문맥이 null인 경우
     * @throws IllegalArgumentException FINAL 판정에 계산 불가 값이 포함된 경우
     */
    public List<AdminOverviewSnapshot.OperationActionItem> calculate(ConsistencyActionContext context) {
        Objects.requireNonNull(context, "context");
        ConsistencyEvaluation evaluation = context.evaluation();
        if (evaluation.phase() == ConsistencyPhase.LIVE) {
            return List.of();
        }
        requireFinalEvaluation(context);
        if (evaluation.overIssued().value() > 0L) {
            // 초과 발급은 일반 gap 불일치보다 고객 영향이 큰 최우선 조치로 표현합니다.
            return List.of(overIssuedAction(context));
        }
        if (evaluation.verdict() == Verdict.PASS) {
            return List.of();
        }
        return List.of(consistencyFailureAction(context));
    }

    /**
     * 캠페인별 최신 FINAL 중 VALID 값만 기존 단건 계산기로 변환합니다.
     *
     * <p>PENDING·UNAVAILABLE·N_A는 값 없는 원래 상태를 보존하고, 복구할 수 없는 VALID 한 건은
     * 해당 캠페인만 UNAVAILABLE로 격리합니다.</p>
     */
    public FinalActionCalculation calculateLatest(
            Map<Long, ConsistencyFinalObservation> observations
    ) {
        Objects.requireNonNull(observations, "observations");
        List<AdminOverviewSnapshot.OperationActionItem> candidates = new ArrayList<>();
        Map<Long, AdminOverviewSnapshot.Observation<
                List<AdminOverviewSnapshot.OperationActionItem>>> calculated = new LinkedHashMap<>();
        observations.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> calculateLatestEntry(entry.getKey(), entry.getValue(), candidates, calculated));
        return new FinalActionCalculation(candidates, calculated);
    }

    private void calculateLatestEntry(
            Long couponId,
            ConsistencyFinalObservation observation,
            List<AdminOverviewSnapshot.OperationActionItem> candidates,
            Map<Long, AdminOverviewSnapshot.Observation<
                    List<AdminOverviewSnapshot.OperationActionItem>>> calculated
    ) {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(observation, "observation");
        if (observation.status() != SourceStatus.VALID) {
            calculated.put(couponId,
                    new AdminOverviewSnapshot.Observation<>(null, observation.status(), null));
            return;
        }
        try {
            ConsistencyActionContext context = observation.value();
            if (!context.couponId().equals(couponId)) {
                throw new IllegalArgumentException("FINAL Map 키와 값의 캠페인 ID가 다릅니다.");
            }
            List<AdminOverviewSnapshot.OperationActionItem> actions = calculate(context);
            candidates.addAll(actions);
            calculated.put(couponId, new AdminOverviewSnapshot.Observation<>(
                    actions, SourceStatus.VALID, context.evaluatedAt()));
        } catch (IllegalArgumentException failure) {
            calculated.put(couponId,
                    new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null));
        }
    }

    /** FINAL 결과의 엔진별 적용성·수치·verdict·severity 불변식을 검증합니다. */
    private static void requireFinalEvaluation(ConsistencyActionContext context) {
        ConsistencyEvaluation evaluation = context.evaluation();
        if (evaluation.overIssued().state() != SourceStatus.VALID) {
            throw new IllegalArgumentException("FINAL 정합성 조치에는 유효한 overIssued 값이 필요합니다.");
        }
        if (evaluation.overIssued().value() < 0L) {
            throw new IllegalArgumentException("FINAL 정합성 조치의 overIssued는 음수일 수 없습니다.");
        }
        for (ConsistencyGapType gapType : ConsistencyGapType.values()) {
            SourceStatus state = evaluation.gaps().get(gapType).state();
            if (gapType.isApplicable(context.engineVersion()) && state != SourceStatus.VALID) {
                throw new IllegalArgumentException("FINAL 적용 gap은 VALID여야 합니다: " + gapType);
            }
            if (!gapType.isApplicable(context.engineVersion()) && state != SourceStatus.N_A) {
                throw new IllegalArgumentException("FINAL 비적용 gap은 N_A여야 합니다: " + gapType);
            }
        }
        boolean failed = evaluation.overIssued().value() > 0L
                || evaluation.gaps().entrySet().stream()
                .filter(entry -> entry.getKey().isApplicable(context.engineVersion()))
                .anyMatch(entry -> entry.getValue().value() != 0L);
        Verdict expectedVerdict = failed ? Verdict.FAIL : Verdict.PASS;
        Severity expectedSeverity = failed ? Severity.CRITICAL : Severity.NONE;
        if (evaluation.verdict() != expectedVerdict) {
            throw new IllegalArgumentException("FINAL verdict가 적용 gap 및 overIssued 값과 일치하지 않습니다.");
        }
        if (evaluation.severity() != expectedSeverity) {
            throw new IllegalArgumentException("FINAL severity가 verdict와 일치하지 않습니다.");
        }
    }

    /** 일반 FINAL 실패를 제한된 고객 영향의 정합성 확인 조치로 만듭니다. */
    private static AdminOverviewSnapshot.OperationActionItem consistencyFailureAction(
            ConsistencyActionContext context
    ) {
        return action(context, AdminOverviewSnapshot.CustomerImpact.LIMITED,
                "정합성 불일치를 확인해야 합니다.", "정합성 확인");
    }

    /** 초과 발급 FINAL 실패를 넓은 고객 영향의 우선 조치로 만듭니다. */
    private static AdminOverviewSnapshot.OperationActionItem overIssuedAction(
            ConsistencyActionContext context
    ) {
        return action(context, AdminOverviewSnapshot.CustomerImpact.WIDESPREAD,
                "초과 발급이 확인되었습니다.", "초과 발급 확인");
    }

    /** 입력 문맥을 보존한 공통 정합성 조치 후보를 만듭니다. */
    private static AdminOverviewSnapshot.OperationActionItem action(
            ConsistencyActionContext context,
            AdminOverviewSnapshot.CustomerImpact customerImpact,
            String customerImpactText,
            String actionText
    ) {
        return new AdminOverviewSnapshot.OperationActionItem(
                context.couponId(),
                context.campaignName(),
                context.opensAt(),
                Severity.CRITICAL,
                customerImpact,
                customerImpactText,
                context.evaluatedAt(),
                null,
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.CONSISTENCY_FAILURE,
                        actionText,
                        AdminOverviewSnapshot.TargetScreen.METRICS));
    }
}

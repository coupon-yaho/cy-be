package com.kafkick.core.admin.overview.calculator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.Severity;

/**
 * 판정 완료된 캠페인별 조치 후보에서 상단 KPI와 우선 노출 목록을 계산합니다.
 *
 * <p>이 계산기는 대기열 중단이나 재고 위험의 임계치를 직접 판정하지 않습니다. 각 원천의 정책이
 * 생성한 조치 후보를 받아 캠페인별 대표 판정을 선택하고, 동일한 모집단에서 KPI와 목록을 함께
 * 생성합니다. Repository나 관측 저장소를 조회하지 않는 순수 계산 경계입니다.</p>
 */
@Component
public class OperationActionCalculator {

    private static final Comparator<AdminOverviewSnapshot.OperationActionItem> ACTION_PRIORITY =
            Comparator.comparing(
                            AdminOverviewSnapshot.OperationActionItem::severity,
                            Comparator.reverseOrder())
                    .thenComparing(
                            AdminOverviewSnapshot.OperationActionItem::detectedAt,
                            Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(AdminOverviewSnapshot.OperationActionItem::couponId);

    private static final Comparator<AdminOverviewSnapshot.OperationActionItem> REPRESENTATIVE_PRIORITY =
            Comparator.comparing(
                            AdminOverviewSnapshot.OperationActionItem::detectedAt,
                            Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(
                            OperationActionCalculator::actionCodeName,
                            Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(
                            AdminOverviewSnapshot.OperationActionItem::customerImpact,
                            Comparator.nullsLast(Comparator.reverseOrder()))
                    .thenComparing(
                            OperationActionCalculator::targetScreenName,
                            Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(
                            AdminOverviewSnapshot.OperationActionItem::duration,
                            Comparator.nullsLast(Comparator.reverseOrder()))
                    .thenComparing(
                            AdminOverviewSnapshot.OperationActionItem::campaignName,
                            Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(
                            AdminOverviewSnapshot.OperationActionItem::opensAt,
                            Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(
                            AdminOverviewSnapshot.OperationActionItem::customerImpactText,
                            Comparator.nullsLast(Comparator.naturalOrder()))
                    .thenComparing(
                            OperationActionCalculator::actionDisplayText,
                            Comparator.nullsLast(Comparator.naturalOrder()));

    /** 상태가 없는 순수 집계 계산기로 생성합니다. */
    public OperationActionCalculator() { }

    /**
     * 조치 후보를 캠페인별로 중복 제거한 뒤 심각도 KPI와 상위 20개 목록을 계산합니다.
     *
     * <p>{@link Severity#WARN WARN}과 {@link Severity#CRITICAL CRITICAL}만 조치 대상으로 사용합니다.
     * 같은 캠페인의 후보가 여러 개이면 최고 심각도를 우선하고, 심각도가 같으면 먼저 감지된 후보를
     * 선택합니다. 전체 건수는 모든 대표 후보를 보존하고 화면 목록만 우선순위에 따라 20건으로
     * 제한합니다.</p>
     *
     * @param decisions 원천별 정책에서 판정한 캠페인 조치 후보
     * @return 같은 중복 제거 결과에서 계산한 조치 KPI와 목록
     * @throws NullPointerException 목록 또는 목록의 원소가 null인 경우
     */
    public ActionCalculation calculate(
            List<AdminOverviewSnapshot.OperationActionItem> decisions
    ) {
        Objects.requireNonNull(decisions, "decisions");
        Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeByCoupon = new HashMap<>();

        for (AdminOverviewSnapshot.OperationActionItem decision : decisions) {
            Objects.requireNonNull(decision, "decisions에는 null을 포함할 수 없습니다.");
            if (!requiresAction(decision)) {
                continue;
            }
            // 한 캠페인의 여러 이상 신호를 동일한 결정 규칙으로 대표 한 건에 축약합니다.
            representativeByCoupon.merge(
                    decision.couponId(),
                    decision,
                    OperationActionCalculator::selectRepresentativeAction);
        }

        // KPI와 화면 목록이 동일 모집단을 사용하도록 대표 Map에서 한 번만 정렬합니다.
        List<AdminOverviewSnapshot.OperationActionItem> actions =
                representativeByCoupon.values().stream()
                        .sorted(ACTION_PRIORITY)
                        .toList();
        long urgentCount = actions.stream()
                .filter(action -> action.severity() == Severity.CRITICAL)
                .count();
        long warningCount = actions.stream()
                .filter(action -> action.severity() == Severity.WARN)
                .count();
        long totalCount = urgentCount + warningCount;

        AdminOverviewSnapshot.ActionRequiredSummary required =
                new AdminOverviewSnapshot.ActionRequiredSummary(
                        totalCount, urgentCount, warningCount);
        AdminOverviewSnapshot.ActionItemSnapshot items =
                new AdminOverviewSnapshot.ActionItemSnapshot(
                        // 전체 건수는 유지하고 화면에 즉시 노출할 목록만 상위 20개로 제한합니다.
                        totalCount, actions.stream().limit(20).toList());
        return new ActionCalculation(required, items, representativeByCoupon);
    }

    /** WARN·CRITICAL로 확정된 후보만 실제 조치 모집단에 포함합니다. */
    private static boolean requiresAction(AdminOverviewSnapshot.OperationActionItem decision) {
        return decision.severity() == Severity.WARN
                || decision.severity() == Severity.CRITICAL;
    }

    /**
     * 동일 캠페인의 여러 판정 중 화면을 대표할 한 건을 결정적으로 선택합니다.
     *
     * <p>심각도가 높은 판정을 우선하고, 심각도가 같으면 감지 시각·행동 코드·고객 영향과 나머지
     * 표시 필드를 순서대로 비교합니다. 모든 의미 필드를 결정성 키로 사용하므로 DB나 원천 조회의
     * 입력 순서가 달라도 같은 대표 판정을 선택합니다.</p>
     */
    private static AdminOverviewSnapshot.OperationActionItem selectRepresentativeAction(
            AdminOverviewSnapshot.OperationActionItem left,
            AdminOverviewSnapshot.OperationActionItem right
    ) {
        int severityComparison = right.severity().compareTo(left.severity());
        if (severityComparison != 0) {
            return severityComparison > 0 ? right : left;
        }

        return REPRESENTATIVE_PRIORITY.compare(left, right) <= 0 ? left : right;
    }

    /** 권장 행동이 없는 후보도 안정적으로 비교할 수 있도록 nullable 행동 코드 이름을 반환합니다. */
    private static String actionCodeName(AdminOverviewSnapshot.OperationActionItem action) {
        if (action.recommendedAction() == null || action.recommendedAction().code() == null) {
            return null;
        }
        return action.recommendedAction().code().name();
    }

    /** 권장 행동의 대상 화면을 대표 후보 결정성 비교에 사용할 nullable 이름으로 반환합니다. */
    private static String targetScreenName(AdminOverviewSnapshot.OperationActionItem action) {
        if (action.recommendedAction() == null || action.recommendedAction().targetScreen() == null) {
            return null;
        }
        return action.recommendedAction().targetScreen().name();
    }

    /** 권장 행동의 표시 문구를 대표 후보 결정성 비교의 마지막 키로 반환합니다. */
    private static String actionDisplayText(AdminOverviewSnapshot.OperationActionItem action) {
        return action.recommendedAction() == null
                ? null
                : action.recommendedAction().displayText();
    }

    /**
     * 조치 KPI와 조치 목록을 같은 중복 제거 결과에서 계산한 값입니다.
     *
     * @param required 전체·긴급·주의 캠페인 수
     * @param items 전체 건수와 우선 노출할 최대 20개 조치 항목
     * @param representativeByCoupon KPI와 목록을 만든 couponId별 대표 조치의 불변 모집단
     */
    public record ActionCalculation(
            AdminOverviewSnapshot.ActionRequiredSummary required,
            AdminOverviewSnapshot.ActionItemSnapshot items,
            Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeByCoupon
    ) {

        /** 대표 모집단을 불변 복사하여 KPI·목록·캠페인 행이 같은 판정을 재사용하게 합니다. */
        public ActionCalculation {
            Objects.requireNonNull(required, "required");
            Objects.requireNonNull(items, "items");
            representativeByCoupon = Map.copyOf(representativeByCoupon);
        }
    }
}

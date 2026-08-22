package com.kafkick.api.admin.dashboard.mock;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.OverviewCalculationPolicy;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueInput;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionContext;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeInput;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowInput;

/**
 * 한 관리자 운영현황 스냅샷에 사용할 Mock 계산 정책·관측 입력·캠페인 원천을 함께 보존합니다.
 *
 * <p>두 목록을 같은 Dataset으로 묶어 캠페인 기본 목록과 그 캠페인에서 파생된 조치 판정이 서로
 * 다른 기준 시각이나 모집단을 사용하지 않도록 합니다.</p>
 *
 * @param policy O1·O2·O4 판정에 공통으로 사용하는 화면 시나리오 정책
 * @param issuanceFlowInputs couponId별 O1 발급 흐름 원천 목록
 * @param queueInputs couponId별 O2 대기열 원천 목록
 * @param outcomeInput 전체 캠페인 O3 고객 결과 원천
 * @param campaigns 캠페인 상태·오픈 임박·재고 계산에 사용할 원천 목록
 * @param preparationActionCandidates 준비 미완료 판정에서 파생된 조치 후보 목록
 * @param consistencyActionContexts FINAL 정합성 조치 계산에 사용할 캠페인별 문맥 목록
 * @param aggregateIssuanceRate 전체 신규 발급 완료율 원천 관측값
 * @param latencySummary 성공·실패 응답 p99 원천 관측값
 */
public record AdminOverviewMockDataset(
        OverviewCalculationPolicy policy,
        List<IssuanceFlowInput> issuanceFlowInputs,
        List<QueueInput> queueInputs,
        OutcomeInput outcomeInput,
        List<CampaignOverviewSource> campaigns,
        List<AdminOverviewSnapshot.OperationActionItem> preparationActionCandidates,
        List<ConsistencyActionContext> consistencyActionContexts,
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.AggregateIssuanceRate> aggregateIssuanceRate,
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.LatencySummary> latencySummary
) {

    /** 외부 변경으로 한 응답의 Mock 모집단이 달라지지 않도록 모든 컬렉션을 불변 복사합니다. */
    public AdminOverviewMockDataset {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(issuanceFlowInputs, "issuanceFlowInputs");
        Objects.requireNonNull(queueInputs, "queueInputs");
        Objects.requireNonNull(outcomeInput, "outcomeInput");
        Objects.requireNonNull(campaigns, "campaigns");
        Objects.requireNonNull(preparationActionCandidates, "preparationActionCandidates");
        Objects.requireNonNull(consistencyActionContexts, "consistencyActionContexts");
        Objects.requireNonNull(aggregateIssuanceRate, "aggregateIssuanceRate");
        Objects.requireNonNull(latencySummary, "latencySummary");
        issuanceFlowInputs = List.copyOf(issuanceFlowInputs);
        queueInputs = List.copyOf(queueInputs);
        campaigns = List.copyOf(campaigns);
        preparationActionCandidates = List.copyOf(preparationActionCandidates);
        consistencyActionContexts = List.copyOf(consistencyActionContexts);
        Set<Long> campaignIds = uniqueCampaignIds(campaigns);
        requireExactlySameCouponIds("issuanceFlowInputs", issuanceFlowInputs.stream()
                .map(IssuanceFlowInput::couponId).toList(), campaignIds);
        requireExactlySameCouponIds("queueInputs", queueInputs.stream()
                .map(QueueInput::couponId).toList(), campaignIds);
        Set<Long> preparationCandidateIds = uniqueCouponIds("preparationActionCandidates",
                preparationActionCandidates.stream()
                        .map(AdminOverviewSnapshot.OperationActionItem::couponId)
                        .toList());
        if (!campaignIds.containsAll(preparationCandidateIds)) {
            throw new IllegalArgumentException("preparationActionCandidates의 couponId는 campaigns의 부분집합이어야 합니다.");
        }
        Set<Long> consistencyContextIds = uniqueCouponIds("consistencyActionContexts", consistencyActionContexts.stream()
                .map(ConsistencyActionContext::couponId)
                .toList());
        if (!campaignIds.containsAll(consistencyContextIds)) {
            throw new IllegalArgumentException("consistencyActionContexts의 couponId는 campaigns의 부분집합이어야 합니다.");
        }
        requireMatchingContextEngines(campaigns, consistencyActionContexts);
    }

    /** 캠페인 행이 정확히 한 번씩만 O1·O2·O4·Action 모집단에 참여하도록 고유 ID를 검증합니다. */
    private static Set<Long> uniqueCampaignIds(List<CampaignOverviewSource> campaigns) {
        return uniqueCouponIds("campaigns", campaigns.stream().map(CampaignOverviewSource::couponId).toList());
    }

    /** 목록의 couponId 중복을 계산기나 Service 경계까지 늦추지 않고 Dataset 생성 시점에 거부합니다. */
    private static Set<Long> uniqueCouponIds(String name, List<Long> couponIds) {
        Set<Long> ids = new HashSet<>();
        for (Long couponId : couponIds) {
            if (couponId == null || !ids.add(couponId)) {
                throw new IllegalArgumentException(name + "에는 null 또는 중복 couponId가 포함될 수 없습니다.");
            }
        }
        return Set.copyOf(ids);
    }

    /** 같은 couponId의 FINAL 문맥과 화면 캠페인이 서로 다른 엔진 계약을 갖지 않도록 검증합니다. */
    private static void requireMatchingContextEngines(
            List<CampaignOverviewSource> campaigns,
            List<ConsistencyActionContext> contexts
    ) {
        Map<Long, CampaignOverviewSource> campaignByCoupon = campaigns.stream()
                .collect(java.util.stream.Collectors.toMap(CampaignOverviewSource::couponId, campaign -> campaign));
        for (ConsistencyActionContext context : contexts) {
            CampaignOverviewSource campaign = campaignByCoupon.get(context.couponId());
            if (campaign.engineVersion() != context.engineVersion()) {
                throw new IllegalArgumentException("consistencyActionContexts의 engineVersion은 campaigns와 일치해야 합니다.");
            }
        }
    }

    /** 삭제가 아닌 UNAVAILABLE/N_A 입력으로만 미수집을 표현하도록 O1·O2 모집단 일치를 강제합니다. */
    private static void requireExactlySameCouponIds(
            String name,
            List<Long> inputCouponIds,
            Set<Long> campaignIds
    ) {
        Set<Long> inputIds = uniqueCouponIds(name, inputCouponIds);
        if (!inputIds.equals(campaignIds)) {
            throw new IllegalArgumentException(name + "의 couponId는 campaigns와 정확히 일치해야 합니다.");
        }
    }
}

package com.kafkick.api.admin.dashboard;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.springframework.stereotype.Service;

import com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataFactory;
import com.kafkick.api.admin.dashboard.mock.AdminOverviewMockDataset;
import com.kafkick.core.admin.overview.AdminOverviewResult;
import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueCalculation;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionContext;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator.CampaignCalculation;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeCalculation;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowCalculation;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator.ActionCalculation;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator.StockInput;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator.StockRiskCalculation;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.support.TimeProvider;

/**
 * 관리자 첫 화면에 필요한 운영현황 조회와 결과 조립 흐름을 담당합니다.
 *
 * <p>현재 캠페인 Repository가 준비되기 전까지 Mock Factory에서 원천을 조회하고, 원천별 결과를
 * Calculator에 전달해 {@link AdminOverviewResult}를 생성합니다. 계산식과 정책 판정은 전용
 * Calculator가 담당하며 Service는 조회 순서와 결과 조립만 조정합니다.</p>
 *
 * <p>캠페인 Repository가 병합되면 Mock Factory 호출을 실제 조회와 캠페인 계산 입력 변환으로
 * 교체합니다. Calculator와 Snapshot 조립은 유지하며, 아직 연결되지 않은 관측 영역은 수치를
 * 추정하지 않고 {@link SourceStatus#UNAVAILABLE}로 제공합니다.</p>
 */
@Service
public class AdminOverviewService {

    private final TimeProvider timeProvider;
    private final AdminOverviewMockDataFactory mockDataFactory;
    private final IssuanceFlowCalculator issuanceFlowCalculator;
    private final IssuanceActionCalculator issuanceActionCalculator;
    private final CampaignQueueCalculator campaignQueueCalculator;
    private final CustomerOutcomeCalculator customerOutcomeCalculator;
    private final StockRiskCalculator stockRiskCalculator;
    private final CampaignOverviewCalculator campaignOverviewCalculator;
    private final ConsistencyActionCalculator consistencyActionCalculator;
    private final OperationActionCalculator operationActionCalculator;
    private final OverviewStatusCalculator overviewStatusCalculator;

    /**
     * Mock 원천 조회와 캠페인·조치·전체 상태 계산에 필요한 협력 객체를 주입받습니다.
     *
     * @param timeProvider 테스트와 운영 환경에서 동일한 시간 계약을 제공하는 공통 공급자
     * @param mockDataFactory Repository 연결 전 캠페인 원천과 조치 후보를 제공하는 Factory
     * @param issuanceFlowCalculator O1 발급 흐름 계산기
     * @param issuanceActionCalculator O1 발급 중단 조치 후보 계산기
     * @param campaignQueueCalculator O2 대기열·대기 위험·조치 후보 계산기
     * @param customerOutcomeCalculator O3 고객 결과 계산기
     * @param stockRiskCalculator O4 V1 재고·소진 위험 계산기
     * @param campaignOverviewCalculator 캠페인 상태·오픈 임박·계산 완료 관측 조립기
     * @param consistencyActionCalculator FINAL 정합성 조치 후보 계산기
     * @param operationActionCalculator 판정 완료 조치 후보의 KPI·목록 집계 계산기
     * @param overviewStatusCalculator 원천 상태를 전체 응답 완전성으로 계산하는 구성요소
     */
    public AdminOverviewService(
            TimeProvider timeProvider,
            AdminOverviewMockDataFactory mockDataFactory,
            IssuanceFlowCalculator issuanceFlowCalculator,
            IssuanceActionCalculator issuanceActionCalculator,
            CampaignQueueCalculator campaignQueueCalculator,
            CustomerOutcomeCalculator customerOutcomeCalculator,
            StockRiskCalculator stockRiskCalculator,
            CampaignOverviewCalculator campaignOverviewCalculator,
            ConsistencyActionCalculator consistencyActionCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator
    ) {
        this.timeProvider = timeProvider;
        this.mockDataFactory = mockDataFactory;
        this.issuanceFlowCalculator = issuanceFlowCalculator;
        this.issuanceActionCalculator = issuanceActionCalculator;
        this.campaignQueueCalculator = campaignQueueCalculator;
        this.customerOutcomeCalculator = customerOutcomeCalculator;
        this.stockRiskCalculator = stockRiskCalculator;
        this.campaignOverviewCalculator = campaignOverviewCalculator;
        this.consistencyActionCalculator = consistencyActionCalculator;
        this.operationActionCalculator = operationActionCalculator;
        this.overviewStatusCalculator = overviewStatusCalculator;
    }

    /**
     * 현재 시점의 관리자 운영현황을 반환합니다.
     *
     * <p>기준 시각과 Dataset을 한 번씩만 만든 뒤 O1, O2, O3, O4를 순서대로 계산합니다. O4는 같은
     * couponId의 O1 계산 결과를 그대로 사용하며, O1·O2·FINAL 정합성 후보와 준비 미완료 후보를 합쳐 Action 계산기를
     * 한 번만 호출합니다. 이후 Action 전체 대표 Map을 캠페인 행 조립에 전달해 KPI·목록·행이 같은
     * 판정을 재사용하도록 합니다. 독립 전체 발급률과 지연 원천은 Dataset Observation을 그대로
     * Snapshot에 전달합니다.</p>
     *
     * @return Snapshot과 전체 데이터 완전성을 포함한 운영현황 Service 결과
     */
    public AdminOverviewResult getOverview() {
        // 한 응답 안의 시간 경계가 달라지지 않도록 기준 시각은 최초 한 번만 조회합니다.
        Instant snapshotAt = timeProvider.instant();
        AdminOverviewMockDataset dataset = mockDataFactory.create(snapshotAt);
        // O1~O3는 서로 독립된 원천을 한 번씩 계산하고 O1 결과는 뒤의 O4에서 재사용합니다.
        IssuanceFlowCalculation issuanceCalculation = issuanceFlowCalculator.calculate(
                dataset.policy(), dataset.issuanceFlowInputs());
        List<AdminOverviewSnapshot.OperationActionItem> issuanceActionCandidates = issuanceActionCalculator
                .calculate(issuanceCalculation.issuanceFlows());
        QueueCalculation queueCalculation = campaignQueueCalculator.calculate(
                dataset.policy(), dataset.queueInputs());
        OutcomeCalculation outcomeCalculation = customerOutcomeCalculator.calculate(dataset.outcomeInput());
        // O4를 별도 발급 조회 없이 같은 couponId의 O1 계산 결과와 재고 원천으로 만듭니다.
        StockRiskCalculation stockCalculation = stockRiskCalculator.calculate(
                dataset.policy(), stockInputs(dataset.campaigns(), issuanceCalculation.issuanceFlows()));
        List<AdminOverviewSnapshot.OperationActionItem> consistencyActionCandidates = new ArrayList<>();
        for (ConsistencyActionContext context : dataset.consistencyActionContexts()) {
            // FINAL 문맥은 요청마다 정확히 한 번만 기존 정책 계산기로 조치 후보에 변환합니다.
            consistencyActionCandidates.addAll(consistencyActionCalculator.calculate(context));
        }
        // O1·O2·FINAL 정합성·준비 미완료 후보를 합쳐 대표 조치를 한 번만 선택합니다.
        List<AdminOverviewSnapshot.OperationActionItem> actionCandidates = actionCandidates(
                dataset.campaigns(), issuanceActionCandidates, queueCalculation.actionCandidates(),
                dataset.preparationActionCandidates(), consistencyActionCandidates);
        ActionCalculation actionCalculation = operationActionCalculator.calculate(actionCandidates);
        // 상단 KPI를 만든 동일 결과 Map으로 캠페인 행을 조립해 화면 영역 간 판정을 맞춥니다.
        CampaignCalculation campaignCalculation = campaignOverviewCalculator.calculate(
                snapshotAt, dataset.campaigns(), issuanceCalculation.issuanceFlows(),
                queueCalculation.queueStatuses(), stockCalculation.stockForecasts(),
                actionCalculation.representativeByCoupon());

        // Dataset 원천의 값·상태·관측 시각을 그대로 유지해 Snapshot 완전성 계산에 사용합니다.
        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                snapshotAt,
                actionObservation(actionCalculation.required(), queueCalculation.queueRisk(),
                        issuanceCalculation.issuanceFlows(), snapshotAt),
                validObservation(campaignCalculation.openingSoon(), snapshotAt),
                queueCalculation.queueRisk(),
                stockCalculation.stockRisk(),
                dataset.aggregateIssuanceRate(),
                queueCalculation.aggregateQueue(),
                dataset.latencySummary(),
                validObservation(campaignCalculation.campaignStatusSummary(), snapshotAt),
                actionObservation(actionCalculation.items(), queueCalculation.queueRisk(),
                        issuanceCalculation.issuanceFlows(), snapshotAt),
                validObservation(campaignCalculation.campaigns(), snapshotAt),
                outcomeCalculation.customerOutcomes()
        );
        return assemble(snapshot);
    }

    /**
     * 캠페인 V1 재고 원천과 같은 couponId의 O1 관측값을 O4 입력으로 변환합니다.
     *
     * <p>예약·종료 캠페인은 O1이 N_A이므로 O4도 N_A로 명시해 전역 위험 모집단에서 제외합니다.
     * 그 밖의 캠페인은 CampaignOverviewSource가 검증한 명시 재고 상태와 수량·관측 시각을 손실 없이
     * 전달하며, 값 없는 상태의 수량을 0으로 보정하지 않습니다.</p>
     */
    private static List<StockInput> stockInputs(
            List<CampaignOverviewSource> campaigns,
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows
    ) {
        List<StockInput> inputs = new ArrayList<>();
        for (CampaignOverviewSource campaign : campaigns) {
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> issuanceFlow =
                    issuanceFlows.get(campaign.couponId());
            SourceStatus stockStatus = issuanceFlow != null && issuanceFlow.status() == SourceStatus.N_A
                    ? SourceStatus.N_A : campaign.stockStatus();
            // 값 없는 상태에는 관측 시각을 싣지 않는 공통 Observation 불변식을 지킵니다.
            Instant observedAt = stockStatus.carriesValue() ? campaign.stockObservedAt() : null;
            inputs.add(new StockInput(campaign.couponId(), campaign.engineVersion(), campaign.totalQuantity(),
                    campaign.activeCount(), stockStatus, observedAt, issuanceFlow));
        }
        return List.copyOf(inputs);
    }

    /**
     * O1·O2·FINAL 정합성과 준비 미완료 후보를 한 Action 계산 호출의 같은 모집단으로 결합합니다.
     *
     * <p>O1·O2 계산기는 기술 중립성을 위해 이름·오픈 시각 없이 후보를 만들 수 있으므로, 이 조립 경계에서
     * 같은 couponId의 기존 캠페인 기본 정보만 채웁니다. 정책에서 확정한 심각도·영향·권장 행동은 바꾸지
     * 않으며, 목록의 상위 20개를 여기서 참조하지 않습니다.</p>
     */
    private static List<AdminOverviewSnapshot.OperationActionItem> actionCandidates(
            List<CampaignOverviewSource> campaigns,
            List<AdminOverviewSnapshot.OperationActionItem> issuanceCandidates,
            List<AdminOverviewSnapshot.OperationActionItem> queueCandidates,
            List<AdminOverviewSnapshot.OperationActionItem> preparationCandidates,
            List<AdminOverviewSnapshot.OperationActionItem> consistencyCandidates
    ) {
        Map<Long, CampaignOverviewSource> campaignByCoupon = campaigns.stream()
                .collect(java.util.stream.Collectors.toMap(CampaignOverviewSource::couponId, campaign -> campaign));
        List<AdminOverviewSnapshot.OperationActionItem> candidates = new ArrayList<>();
        candidates.addAll(withCampaignDisplay(issuanceCandidates, campaignByCoupon));
        candidates.addAll(withCampaignDisplay(queueCandidates, campaignByCoupon));
        candidates.addAll(preparationCandidates);
        candidates.addAll(consistencyCandidates);
        return List.copyOf(candidates);
    }

    /** 기술 중립 O1·O2 후보에 동일 couponId의 화면 표시용 캠페인 정보를 보강합니다. */
    private static List<AdminOverviewSnapshot.OperationActionItem> withCampaignDisplay(
            List<AdminOverviewSnapshot.OperationActionItem> candidates,
            Map<Long, CampaignOverviewSource> campaignByCoupon
    ) {
        List<AdminOverviewSnapshot.OperationActionItem> displayedCandidates = new ArrayList<>();
        for (AdminOverviewSnapshot.OperationActionItem candidate : candidates) {
            CampaignOverviewSource campaign = campaignByCoupon.get(candidate.couponId());
            // 기술 중립 후보에 화면 표시용 캠페인 이름과 오픈 시각만 보강합니다.
            displayedCandidates.add(new AdminOverviewSnapshot.OperationActionItem(candidate.couponId(),
                    campaign == null ? candidate.campaignName() : campaign.campaignName(),
                    campaign == null ? candidate.opensAt() : campaign.opensAt(), candidate.severity(),
                    candidate.customerImpact(), candidate.customerImpactText(), candidate.detectedAt(),
                    candidate.duration(), candidate.recommendedAction()));
        }
        return List.copyOf(displayedCandidates);
    }

    /**
     * 각 원천에서 계산된 운영 값과 전체 완전성을 하나의 Service 결과로 조립합니다.
     *
     * <p>후속 Repository·관측 조회가 준비되면 완성된 Snapshot을 이 경계로 전달합니다. 전체 완전성은
     * {@link OverviewStatusCalculator}에 위임하고, HTTP DTO 변환은 이 결과를 받는 Controller가
     * 담당합니다.</p>
     *
     * @param snapshot 캠페인·관측 원천별 계산이 끝난 기술 중립 결과
     * @return 계산된 Snapshot과 전체 완전성을 함께 보존한 Service 결과
     */
    public AdminOverviewResult assemble(AdminOverviewSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        OverallStatus overallStatus = overviewStatusCalculator.calculate(snapshot);
        return new AdminOverviewResult(snapshot, overallStatus);
    }

    /** 계산이 끝난 값을 공통 Core 관측 계약의 정상 상태와 기준 시각으로 감쌉니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> validObservation(
            T value,
            Instant observedAt
    ) {
        return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, observedAt);
    }

    /** O1·O2 모집단 완전성을 Action KPI·목록 상태에 반영하되 확정 대표 Map은 행 조립에 유지합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> actionObservation(
            T value,
            AdminOverviewSnapshot.Observation<?> queueRisk,
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows,
            Instant snapshotAt
    ) {
        List<AdminOverviewSnapshot.Observation<?>> sources = new ArrayList<>();
        sources.add(queueRisk);
        sources.addAll(issuanceFlows.values());
        SourceStatus status = actionSourceStatus(sources);
        if (status == SourceStatus.N_A) {
            // 적용 대상이 없는 O1·O2 원천은 준비 미완료 같은 별도 후보의 완전성을 낮추지 않습니다.
            return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, snapshotAt);
        }
        if (!status.carriesValue()) {
            // O1·O2 적용 모집단이 불완전하면 계산 가능한 일부를 전체 KPI처럼 노출하지 않습니다.
            return new AdminOverviewSnapshot.Observation<>(null, status, null);
        }
        Instant observedAt = sources.stream()
                .filter(source -> source.status() != SourceStatus.N_A)
                .map(AdminOverviewSnapshot.Observation::observedAt)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(snapshotAt);
        if (status == SourceStatus.STALE || status == SourceStatus.WARMING_UP) {
            // 참고 가능한 값은 보존하되 가장 오래된 원천 시각으로 최신 판정이 아님을 드러냅니다.
            return new AdminOverviewSnapshot.Observation<>(value, status, observedAt);
        }
        return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, snapshotAt);
    }

    /** 적용 대상 N_A를 제외한 O1·O2 상태에서 Action 모집단의 완전성을 합성합니다. */
    private static SourceStatus actionSourceStatus(List<AdminOverviewSnapshot.Observation<?>> sources) {
        List<SourceStatus> statuses = sources.stream()
                .map(AdminOverviewSnapshot.Observation::status)
                .filter(status -> status != SourceStatus.N_A)
                .toList();
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

}

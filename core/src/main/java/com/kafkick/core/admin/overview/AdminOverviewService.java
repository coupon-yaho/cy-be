package com.kafkick.core.admin.overview;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.campaignsource.AdminCampaignCatalog;
import com.kafkick.core.admin.campaignsource.AdminCampaignDataReader;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator.CampaignCalculation;
import com.kafkick.core.admin.overview.calculator.CampaignOverviewCalculator.PreparationCalculation;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator;
import com.kafkick.core.admin.overview.calculator.CampaignQueueCalculator.QueueCalculation;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator;
import com.kafkick.core.admin.overview.calculator.CustomerOutcomeCalculator.OutcomeCalculation;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator;
import com.kafkick.core.admin.overview.calculator.ConsistencyActionCalculator.FinalActionCalculation;
import com.kafkick.core.admin.overview.calculator.IssuanceActionCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator;
import com.kafkick.core.admin.overview.calculator.IssuanceFlowCalculator.IssuanceFlowCalculation;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator;
import com.kafkick.core.admin.overview.calculator.OperationActionCalculator.ActionCalculation;
import com.kafkick.core.admin.overview.calculator.OverviewStatusCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator.StockInput;
import com.kafkick.core.admin.overview.calculator.StockRiskCalculator.StockRiskCalculation;
import com.kafkick.core.admin.overview.observation.CampaignObservationTarget;
import com.kafkick.core.admin.overview.observation.OverviewObservationData;
import com.kafkick.core.admin.overview.observation.OverviewObservationRequest;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.consistency.ConsistencyFinalObservation;
import com.kafkick.core.consistency.ConsistencyFinalReader;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.runtimeconfig.RuntimeConfigSnapshot;
import com.kafkick.core.runtimeconfig.RuntimeConfigStore;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

/**
 * 관리자 첫 화면에 필요한 운영현황 조회와 결과 조립 흐름을 담당합니다.
 *
 * <p>캠페인·재고는 {@link AdminCampaignDataReader}, O1 발급 흐름·O3 고객 결과·지연·전체 발급률은
 * {@link OverviewObservationSource}에서 읽습니다. 아직 연결되지 않은 대기열은 PENDING이며,
 * FINAL은 적용 모집단이 완전할 때만 대표 조치 모집단에 포함합니다.</p>
 *
 * <p>API 전용 관측 어댑터를 Core나 Batch에 강제하지 않도록 이 클래스는 Spring 컴포넌트가 아닌
 * 기술 중립 서비스이며, API 설정이 실제 관측 원천과 함께 명시적으로 bean을 소유합니다.</p>
 */
public class AdminOverviewService {

    private final TimeProvider timeProvider;
    private final AdminCampaignDataReader campaignDataReader;
    private final RuntimeConfigStore runtimeConfigStore;
    private final OverviewCalculationPolicy policy;
    private final OverviewObservationSource observationSource;
    private final IssuanceFlowCalculator issuanceFlowCalculator;
    private final IssuanceActionCalculator issuanceActionCalculator;
    private final CampaignQueueCalculator campaignQueueCalculator;
    private final CustomerOutcomeCalculator customerOutcomeCalculator;
    private final StockRiskCalculator stockRiskCalculator;
    private final CampaignOverviewCalculator campaignOverviewCalculator;
    private final ConsistencyFinalReader consistencyFinalReader;
    private final ConsistencyActionCalculator consistencyActionCalculator;
    private final OperationActionCalculator operationActionCalculator;
    private final OverviewStatusCalculator overviewStatusCalculator;

    /**
     * DB 캠페인 원천, 현재 Runtime 설정과 계산 협력 객체를 주입받습니다.
     *
     * @param timeProvider 테스트와 운영 환경에서 동일한 시간 계약을 제공하는 공통 공급자
     * @param campaignDataReader DB 캠페인·재고 모집단 조회 경계
     * @param runtimeConfigStore 요청당 한 번 읽을 현재 엔진 설정
     * @param policy 재검토 가능한 운영 판정 임계치
     * @param observationSource 같은 Snapshot 모집단의 O1·O3·전체 발급률·지연 관측 원천
     * @param issuanceFlowCalculator O1 발급 흐름 계산기
     * @param issuanceActionCalculator O1 발급 중단 조치 후보 계산기
     * @param campaignQueueCalculator O2 대기열·대기 위험·조치 후보 계산기
     * @param customerOutcomeCalculator O3 고객 결과 계산기
     * @param stockRiskCalculator O4 V1 재고·소진 위험 계산기
     * @param campaignOverviewCalculator 캠페인 상태·오픈 임박·계산 완료 관측 조립기
     * @param consistencyFinalReader 회차 모집단의 최신 FINAL 일괄 조회 경계
     * @param consistencyActionCalculator FINAL 결과의 조치 후보 변환 계산기
     * @param operationActionCalculator 판정 완료 조치 후보의 KPI·목록 집계 계산기
     * @param overviewStatusCalculator 원천 상태를 전체 응답 완전성으로 계산하는 구성요소
     */
    public AdminOverviewService(
            TimeProvider timeProvider,
            AdminCampaignDataReader campaignDataReader,
            RuntimeConfigStore runtimeConfigStore,
            OverviewCalculationPolicy policy,
            OverviewObservationSource observationSource,
            IssuanceFlowCalculator issuanceFlowCalculator,
            IssuanceActionCalculator issuanceActionCalculator,
            CampaignQueueCalculator campaignQueueCalculator,
            CustomerOutcomeCalculator customerOutcomeCalculator,
            StockRiskCalculator stockRiskCalculator,
            CampaignOverviewCalculator campaignOverviewCalculator,
            ConsistencyFinalReader consistencyFinalReader,
            ConsistencyActionCalculator consistencyActionCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator
    ) {
        this.timeProvider = timeProvider;
        this.campaignDataReader = Objects.requireNonNull(campaignDataReader, "campaignDataReader");
        this.runtimeConfigStore = Objects.requireNonNull(runtimeConfigStore, "runtimeConfigStore");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.observationSource = observationSource;
        this.issuanceFlowCalculator = issuanceFlowCalculator;
        this.issuanceActionCalculator = issuanceActionCalculator;
        this.campaignQueueCalculator = campaignQueueCalculator;
        this.customerOutcomeCalculator = customerOutcomeCalculator;
        this.stockRiskCalculator = stockRiskCalculator;
        this.campaignOverviewCalculator = campaignOverviewCalculator;
        this.consistencyFinalReader = Objects.requireNonNull(
                consistencyFinalReader, "consistencyFinalReader");
        this.consistencyActionCalculator = Objects.requireNonNull(
                consistencyActionCalculator, "consistencyActionCalculator");
        this.operationActionCalculator = operationActionCalculator;
        this.overviewStatusCalculator = overviewStatusCalculator;
    }

    /**
     * 현재 시점의 관리자 운영현황을 반환합니다.
     *
     * <p>기준 시각, DB catalog, Runtime 현재값과 관측 묶음을 각각 한 번 읽습니다. DB couponId 모집단을
     * O1 target과 캠페인 행에 함께 사용하며, 진행 캠페인의 미연결 O2는 PENDING으로 보존합니다.
     * O1·O2·준비·완전한 FINAL 후보의 대표 Action 계산은 한 번만 수행하고 O3·전체 발급률·지연은
     * 관측 묶음의 상태와 시각을 그대로 전달합니다.</p>
     *
     * @return Snapshot과 전체 데이터 완전성을 포함한 운영현황 Service 결과
     */
    public AdminOverviewResult getOverview() {
        Instant snapshotAt = timeProvider.instant();
        AdminCampaignCatalog catalog = campaignDataReader.loadCatalog(snapshotAt);
        // 현재값과 last-known-good를 섞지 않도록 요청마다 get() 결과 하나만 사용합니다.
        RuntimeConfigSnapshot runtimeConfig = runtimeConfigStore.get();
        List<CampaignOverviewSource> campaigns = campaigns(catalog, runtimeConfig);
        List<Long> couponIds = catalog.campaigns().stream()
                .map(AdminCampaignCatalog.CampaignData::couponId)
                .toList();
        Map<Long, ConsistencyFinalObservation> finalObservations =
                consistencyFinalReader.findLatestByCouponIds(couponIds);
        requireSameFinalPopulation(couponIds, finalObservations);
        FinalActionCalculation finalCalculation =
                consistencyActionCalculator.calculateLatest(finalObservations);
        List<AdminOverviewSnapshot.Observation<?>> finalActionObservations = new ArrayList<>();
        if (finalCalculation.isComplete()) {
            // 적용 가능한 FINAL만 Action 원천 시각 합성에 포함합니다.
            finalActionObservations.addAll(finalCalculation.observations().values());
        }
        OverviewObservationRequest observationRequest = observationRequest(snapshotAt, catalog);
        OverviewObservationData observationData = observationSource.observe(observationRequest);
        if (!observationRequest.equals(observationData.request())) {
            throw new BusinessException(
                    AdminOverviewErrorCode.OBSERVATION_REQUEST_MISMATCH,
                    "관측 응답 request가 현재 Overview 요청과 일치해야 합니다.");
        }
        IssuanceFlowCalculation issuanceCalculation = issuanceFlowCalculator.calculate(
                policy, observationData.issuanceFlowInputs());
        List<AdminOverviewSnapshot.OperationActionItem> issuanceActionCandidates = issuanceActionCalculator
                .calculate(issuanceCalculation.issuanceFlows());
        QueueCalculation queueCalculation = campaignQueueCalculator.calculate(
                policy, pendingQueueInputs(campaigns));
        OutcomeCalculation outcomeCalculation = customerOutcomeCalculator.calculate(observationData.outcomeInput());
        StockRiskCalculation stockCalculation = stockRiskCalculator.calculate(
                policy, stockInputs(campaigns, issuanceCalculation.issuanceFlows()));
        PreparationCalculation preparationCalculation = campaignOverviewCalculator.calculatePreparation(
                snapshotAt, campaigns);
        boolean catalogAvailable = catalog.status() == SourceStatus.VALID;
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.OpeningSoonSummary> preparationObservation =
                catalogAvailable
                        ? preparationCalculation.openingSoon()
                        : emptyObservation(catalog.status());
        // 준비 확정 후보도 O1·O2와 같은 대표 조치 모집단에서 한 번만 선택합니다.
        List<AdminOverviewSnapshot.OperationActionItem> actionCandidates = actionCandidates(
                campaigns, issuanceActionCandidates, queueCalculation.actionCandidates(),
                preparationCalculation.actionCandidates(),
                finalCalculation.isComplete() ? finalCalculation.actionCandidates() : List.of());
        ActionCalculation actionCalculation = operationActionCalculator.calculate(actionCandidates);
        CampaignCalculation campaignCalculation = campaignOverviewCalculator.calculate(
                snapshotAt, campaigns, issuanceCalculation.issuanceFlows(),
                queueCalculation.queueStatuses(), stockCalculation.stockForecasts(),
                actionCalculation.representativeByCoupon());

        AdminOverviewSnapshot snapshot = new AdminOverviewSnapshot(
                snapshotAt,
                actionObservation(actionCalculation.required(), queueCalculation.queueRisk(),
                        issuanceCalculation.issuanceFlows(), preparationObservation,
                        finalActionObservations, snapshotAt),
                preparationObservation,
                queueCalculation.queueRisk(),
                catalogAvailable ? stockCalculation.stockRisk() : emptyObservation(catalog.status()),
                observationData.aggregateIssuanceRate(),
                queueCalculation.aggregateQueue(),
                observationData.latencySummary(),
                catalogAvailable
                        ? validObservation(campaignCalculation.campaignStatusSummary(), catalog.observedAt())
                        : emptyObservation(catalog.status()),
                actionObservation(actionCalculation.items(), queueCalculation.queueRisk(),
                        issuanceCalculation.issuanceFlows(), preparationObservation,
                        finalActionObservations, snapshotAt),
                catalogAvailable
                        ? validObservation(campaignCalculation.campaigns(), catalog.observedAt())
                        : emptyObservation(catalog.status()),
                outcomeCalculation.customerOutcomes()
        );
        return assemble(snapshot);
    }

    /** FINAL 조회 응답이 현재 DB 회차 모집단과 정확히 같은 키를 가졌는지 확인합니다. */
    private static void requireSameFinalPopulation(
            List<Long> requestedIds,
            Map<Long, ConsistencyFinalObservation> observations
    ) {
        Set<Long> requested = new LinkedHashSet<>(requestedIds);
        if (observations == null || observations.size() != requested.size()
                || !requested.equals(observations.keySet())) {
            throw new BusinessException(
                    AdminOverviewErrorCode.OBSERVATION_REQUEST_MISMATCH,
                    "FINAL 관측 응답이 현재 Overview 회차 모집단과 일치해야 합니다.");
        }
    }

    /** DB 캠페인 모집단을 같은 Snapshot의 기술 중립 관측 요청으로 변환합니다. */
    private OverviewObservationRequest observationRequest(
            Instant snapshotAt,
            AdminCampaignCatalog catalog
    ) {
        List<CampaignObservationTarget> targets = catalog.campaigns().stream()
                .map(AdminOverviewService::observationTarget)
                .toList();
        return new OverviewObservationRequest(snapshotAt, targets, policy);
    }

    /** DB 캠페인 ID와 재고 상태를 변경 없이 O1 관측 target으로 변환합니다. */
    private static CampaignObservationTarget observationTarget(AdminCampaignCatalog.CampaignData campaign) {
        Boolean stockAvailable = null;
        SourceStatus stockStatus = SourceStatus.N_A;
        if (campaign.status() == CouponRoundStatus.OPEN) {
            stockStatus = campaign.stock().status();
        }
        if (campaign.status() == CouponRoundStatus.OPEN && stockStatus.carriesValue()) {
            stockAvailable = campaign.stock().value().activeCount()
                    < campaign.stock().value().totalQuantity();
        }
        return new CampaignObservationTarget(
                campaign.couponId(), campaign.status(), stockAvailable, stockStatus,
                stockStatus.carriesValue() ? campaign.stock().observedAt() : null);
    }

    /** DB catalog를 기존 계산기 입력으로 변환하고 Runtime 현재값의 원천 상태를 재고 판정에 반영합니다. */
    private static List<CampaignOverviewSource> campaigns(
            AdminCampaignCatalog catalog,
            RuntimeConfigSnapshot runtimeConfig
    ) {
        if (catalog.status() != SourceStatus.VALID) {
            return List.of();
        }
        return catalog.campaigns().stream()
                .map(campaign -> campaignSource(campaign, runtimeConfig))
                .toList();
    }

    /** DB 메타·재고와 요청에서 한 번 읽은 엔진 설정을 한 캠페인 계산 원천으로 조립합니다. */
    private static CampaignOverviewSource campaignSource(
            AdminCampaignCatalog.CampaignData campaign,
            RuntimeConfigSnapshot runtimeConfig
    ) {
        SourceStatus stockStatus = campaign.status() == CouponRoundStatus.OPEN
                ? campaign.stock().status() : SourceStatus.N_A;
        if (campaign.status() == CouponRoundStatus.OPEN && !runtimeConfig.status().carriesValue()) {
            stockStatus = SourceStatus.UNAVAILABLE;
        }
        boolean carriesStock = stockStatus.carriesValue();
        return new CampaignOverviewSource(
                campaign.couponId(), campaign.campaignName(), campaign.brandName(), campaign.status(),
                campaign.opensAt(), campaign.closesAt(), runtimeConfig.engineVersion(),
                carriesStock ? campaign.stock().value().totalQuantity() : null,
                carriesStock ? campaign.stock().value().activeCount() : null,
                carriesStock ? campaign.stock().observedAt() : null,
                stockStatus, campaign.preparation());
    }

    /** Redis 대기열 연결 전에는 진행 캠페인만 PENDING, 아직 열리지 않았거나 종료된 캠페인은 N_A로 둡니다. */
    private static List<CampaignQueueCalculator.QueueInput> pendingQueueInputs(
            List<CampaignOverviewSource> campaigns
    ) {
        return campaigns.stream()
                .map(campaign -> {
                    // O2가 적용되는 진행 캠페인만 미연결 PENDING으로 두어 예약 캠페인 준비 조치를 가리지 않습니다.
                    SourceStatus sourceStatus = campaign.status() == CouponRoundStatus.OPEN
                            ? SourceStatus.PENDING : SourceStatus.N_A;
                    return new CampaignQueueCalculator.QueueInput(
                            campaign.couponId(), null, null, null, null, null,
                            null, null, sourceStatus, null);
                })
                .toList();
    }

    /**
     * DB 재고와 같은 요청의 O1 결과를 O4 입력으로 변환합니다.
     *
     * <p>예약·종료 캠페인은 O1이 N_A이므로 O4도 N_A로 명시해 전역 위험 모집단에서 제외합니다.
     * 그 밖의 캠페인은 CampaignOverviewSource가
     * 검증한 명시 재고 상태와 수량·관측 시각을 손실 없이 전달하며, 값 없는 상태의 수량을 0으로
     * 보정하지 않습니다.</p>
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
     * O1·O2·준비·FINAL 후보를 한 Action 계산 호출의 같은 모집단으로 결합합니다.
     *
     * <p>O1·O2 계산기는 기술 중립성을 위해 이름·오픈 시각 없이 후보를 만들 수 있으므로, 이 조립 경계에서
     * 같은 couponId의 기존 캠페인 기본 정보만 채웁니다. 정책에서 확정한 심각도·영향·권장 행동은 바꾸지
     * 않으며, 준비 후보도 같은 전체 모집단에 넣고 목록의 상위 20개를 여기서 참조하지 않습니다.</p>
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
        // DB에서 확정한 준비 후보를 O1·O2 후보와 같은 대표 판정에 포함합니다.
        candidates.addAll(preparationCandidates);
        // FINAL 저장 당시 표시값 대신 같은 Overview 요청의 현재 캠페인 표시값을 사용합니다.
        candidates.addAll(withCampaignDisplay(consistencyCandidates, campaignByCoupon));
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
     * <p>DB·관측 경계에서 계산을 마친 Snapshot을 이 메서드로 전달합니다. 전체 완전성은
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

    /** 값 없는 catalog 상태를 숫자나 빈 정상 목록으로 바꾸지 않고 Snapshot에 전달합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> emptyObservation(SourceStatus status) {
        return new AdminOverviewSnapshot.Observation<>(null, status, null);
    }

    /** O1·O2·준비 모집단 완전성을 Action KPI·목록 상태에 반영하되 대표 Map은 행 조립에 유지합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> actionObservation(
            T value,
            AdminOverviewSnapshot.Observation<?> queueRisk,
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows,
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.OpeningSoonSummary> preparation,
            List<AdminOverviewSnapshot.Observation<?>> finalObservations,
            Instant snapshotAt
    ) {
        List<AdminOverviewSnapshot.Observation<?>> sources = new ArrayList<>();
        sources.add(queueRisk);
        sources.addAll(issuanceFlows.values());
        // 준비 여부를 모르는 캠페인이 있으면 빈 정상 조치 목록으로 보정하지 않습니다.
        sources.add(preparation);
        sources.addAll(finalObservations);
        SourceStatus status = actionSourceStatus(sources);
        if (status == SourceStatus.N_A) {
            // 적용 대상이 없는 O1·O2·준비 원천은 확정된 별도 후보의 완전성을 낮추지 않습니다.
            return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, snapshotAt);
        }
        if (!status.carriesValue()) {
            // 적용 모집단이 불완전하면 계산 가능한 일부를 전체 KPI처럼 노출하지 않습니다.
            return new AdminOverviewSnapshot.Observation<>(null, status, null);
        }
        Instant observedAt = sources.stream()
                .filter(source -> source.status() != SourceStatus.N_A)
                .map(AdminOverviewSnapshot.Observation::observedAt)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(snapshotAt);
        if (status != SourceStatus.VALID) {
            // 값이 있는 비정상 상태는 가장 오래된 원천 시각과 함께 원래 의미를 보존합니다.
            return new AdminOverviewSnapshot.Observation<>(value, status, observedAt);
        }
        return new AdminOverviewSnapshot.Observation<>(value, SourceStatus.VALID, observedAt);
    }

    /** 적용 대상 N_A를 제외한 O1·O2·준비 상태에서 Action 모집단의 완전성을 합성합니다. */
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

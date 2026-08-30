package com.kafkick.core.admin.overview;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundCatalog;
import com.kafkick.core.admin.couponroundsource.AdminCouponRoundDataReader;
import com.kafkick.core.admin.overview.calculator.CouponRoundOverviewCalculator;
import com.kafkick.core.admin.overview.calculator.CouponRoundOverviewCalculator.CouponRoundCalculation;
import com.kafkick.core.admin.overview.calculator.CouponRoundOverviewCalculator.PreparationCalculation;
import com.kafkick.core.admin.overview.calculator.CouponRoundPreparationCalculator;
import com.kafkick.core.admin.overview.calculator.CouponRoundQueueCalculator;
import com.kafkick.core.admin.overview.calculator.CouponRoundQueueCalculator.QueueCalculation;
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
import com.kafkick.core.admin.overview.observation.CouponRoundObservationTarget;
import com.kafkick.core.admin.overview.observation.OverviewObservationData;
import com.kafkick.core.admin.overview.observation.OverviewObservationRequest;
import com.kafkick.core.admin.overview.observation.OverviewObservationSource;
import com.kafkick.core.admin.queue.AdminQueueObservationSource;
import com.kafkick.core.admin.queue.CouponRoundQueueObservation;
import com.kafkick.core.admin.preparation.AdminPreparationResolver;
import com.kafkick.core.admin.preparation.V2PreparationSource;
import com.kafkick.core.admin.stock.AdminStockResolver;
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
 * <p>쿠폰 회차·재고는 {@link AdminCouponRoundDataReader}, O1 발급 흐름·O3 고객 결과·지연·전체 발급률은
 * {@link OverviewObservationSource}에서 읽고, O2 대기열은 공통 관측 원천에서 읽습니다.
 * FINAL은 적용 모집단이 완전할 때만 대표 조치 모집단에 포함합니다.</p>
 *
 * <p>API 전용 관측 어댑터를 Core나 Batch에 강제하지 않도록 이 클래스는 Spring 컴포넌트가 아닌
 * 기술 중립 서비스이며, API 설정이 실제 관측 원천과 함께 명시적으로 bean을 소유합니다.</p>
 */
public class AdminOverviewService {

    private final TimeProvider timeProvider;
    private final AdminCouponRoundDataReader couponRoundDataReader;
    private final RuntimeConfigStore runtimeConfigStore;
    private final OverviewCalculationPolicy policy;
    private final OverviewObservationSource observationSource;
    private final AdminQueueObservationSource queueObservationSource;
    private final IssuanceFlowCalculator issuanceFlowCalculator;
    private final IssuanceActionCalculator issuanceActionCalculator;
    private final CouponRoundQueueCalculator couponRoundQueueCalculator;
    private final CustomerOutcomeCalculator customerOutcomeCalculator;
    private final StockRiskCalculator stockRiskCalculator;
    private final CouponRoundOverviewCalculator couponRoundOverviewCalculator;
    private final CouponRoundPreparationCalculator couponRoundPreparationCalculator;
    private final ConsistencyFinalReader consistencyFinalReader;
    private final ConsistencyActionCalculator consistencyActionCalculator;
    private final OperationActionCalculator operationActionCalculator;
    private final OverviewStatusCalculator overviewStatusCalculator;
    private final AdminStockResolver stockResolver;
    private final AdminPreparationResolver preparationResolver;

    /**
     * DB 쿠폰 회차 원천, 현재 Runtime 설정과 계산 협력 객체를 주입받습니다.
     *
     * @param timeProvider 테스트와 운영 환경에서 동일한 시간 계약을 제공하는 공통 공급자
     * @param couponRoundDataReader DB 쿠폰 회차·재고 모집단 조회 경계
     * @param runtimeConfigStore 요청당 한 번 읽을 현재 엔진 설정
     * @param policy 재검토 가능한 운영 판정 임계치
     * @param observationSource 같은 Snapshot 모집단의 O1·O3·전체 발급률·지연 관측 원천
     * @param queueObservationSource OPEN 쿠폰 회차의 O2 대기열 관측 원천
     * @param issuanceFlowCalculator O1 발급 흐름 계산기
     * @param issuanceActionCalculator O1 발급 중단 조치 후보 계산기
     * @param couponRoundQueueCalculator O2 대기열·대기 위험·조치 후보 계산기
     * @param customerOutcomeCalculator O3 고객 결과 계산기
     * @param stockRiskCalculator O4 V1 재고·소진 위험 계산기
     * @param couponRoundOverviewCalculator 쿠폰 회차 상태·오픈 임박·계산 완료 관측 조립기
     * @param couponRoundPreparationCalculator DB 회차 엔진과 V2 Redis 판정을 결합한 준비 항목 계산기
     * @param consistencyFinalReader 회차 모집단의 최신 FINAL 일괄 조회 경계
     * @param consistencyActionCalculator FINAL 결과의 조치 후보 변환 계산기
     * @param operationActionCalculator 판정 완료 조치 후보의 KPI·목록 집계 계산기
     * @param overviewStatusCalculator 원천 상태를 전체 응답 완전성으로 계산하는 구성요소
     */
    public AdminOverviewService(
            TimeProvider timeProvider,
            AdminCouponRoundDataReader couponRoundDataReader,
            RuntimeConfigStore runtimeConfigStore,
            OverviewCalculationPolicy policy,
            OverviewObservationSource observationSource,
            AdminQueueObservationSource queueObservationSource,
            IssuanceFlowCalculator issuanceFlowCalculator,
            IssuanceActionCalculator issuanceActionCalculator,
            CouponRoundQueueCalculator couponRoundQueueCalculator,
            CustomerOutcomeCalculator customerOutcomeCalculator,
            StockRiskCalculator stockRiskCalculator,
            CouponRoundOverviewCalculator couponRoundOverviewCalculator,
            CouponRoundPreparationCalculator couponRoundPreparationCalculator,
            ConsistencyFinalReader consistencyFinalReader,
            ConsistencyActionCalculator consistencyActionCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator
    ) {
        this(timeProvider, couponRoundDataReader, runtimeConfigStore, policy, observationSource,
                queueObservationSource, issuanceFlowCalculator, issuanceActionCalculator,
                couponRoundQueueCalculator, customerOutcomeCalculator, stockRiskCalculator,
                couponRoundOverviewCalculator, couponRoundPreparationCalculator, consistencyFinalReader,
                consistencyActionCalculator, operationActionCalculator, overviewStatusCalculator,
                new AdminStockResolver(AdminStockResolver.unavailableV2Reader()),
                new AdminPreparationResolver(AdminPreparationResolver.unavailableV2Reader()));
    }

    /** 운영 배선에서 회차별 DB·Redis 재고 선택기와 V2 준비 상태 선택기를 함께 주입받습니다. */
    public AdminOverviewService(
            TimeProvider timeProvider,
            AdminCouponRoundDataReader couponRoundDataReader,
            RuntimeConfigStore runtimeConfigStore,
            OverviewCalculationPolicy policy,
            OverviewObservationSource observationSource,
            AdminQueueObservationSource queueObservationSource,
            IssuanceFlowCalculator issuanceFlowCalculator,
            IssuanceActionCalculator issuanceActionCalculator,
            CouponRoundQueueCalculator couponRoundQueueCalculator,
            CustomerOutcomeCalculator customerOutcomeCalculator,
            StockRiskCalculator stockRiskCalculator,
            CouponRoundOverviewCalculator couponRoundOverviewCalculator,
            CouponRoundPreparationCalculator couponRoundPreparationCalculator,
            ConsistencyFinalReader consistencyFinalReader,
            ConsistencyActionCalculator consistencyActionCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator,
            AdminStockResolver stockResolver,
            AdminPreparationResolver preparationResolver
    ) {
        this.timeProvider = timeProvider;
        this.couponRoundDataReader = Objects.requireNonNull(couponRoundDataReader, "couponRoundDataReader");
        this.runtimeConfigStore = Objects.requireNonNull(runtimeConfigStore, "runtimeConfigStore");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.observationSource = Objects.requireNonNull(observationSource, "observationSource");
        this.queueObservationSource = Objects.requireNonNull(queueObservationSource, "queueObservationSource");
        this.issuanceFlowCalculator = Objects.requireNonNull(issuanceFlowCalculator, "issuanceFlowCalculator");
        this.issuanceActionCalculator = Objects.requireNonNull(issuanceActionCalculator, "issuanceActionCalculator");
        this.couponRoundQueueCalculator = Objects.requireNonNull(couponRoundQueueCalculator, "couponRoundQueueCalculator");
        this.customerOutcomeCalculator = Objects.requireNonNull(customerOutcomeCalculator, "customerOutcomeCalculator");
        this.stockRiskCalculator = Objects.requireNonNull(stockRiskCalculator, "stockRiskCalculator");
        this.couponRoundOverviewCalculator = Objects.requireNonNull(
                couponRoundOverviewCalculator, "couponRoundOverviewCalculator");
        this.couponRoundPreparationCalculator = Objects.requireNonNull(
                couponRoundPreparationCalculator, "couponRoundPreparationCalculator");
        this.consistencyFinalReader = Objects.requireNonNull(
                consistencyFinalReader, "consistencyFinalReader");
        this.consistencyActionCalculator = Objects.requireNonNull(
                consistencyActionCalculator, "consistencyActionCalculator");
        this.operationActionCalculator = Objects.requireNonNull(operationActionCalculator, "operationActionCalculator");
        this.overviewStatusCalculator = Objects.requireNonNull(overviewStatusCalculator, "overviewStatusCalculator");
        this.stockResolver = Objects.requireNonNull(stockResolver, "stockResolver");
        this.preparationResolver = Objects.requireNonNull(preparationResolver, "preparationResolver");
    }

    /**
     * 이전 조립 코드가 새 준비 계산기를 넘기지 않을 때 기본 순수 계산기를 연결합니다.
     *
     * @deprecated 생산 설정은 {@link CouponRoundPreparationCalculator}를 명시적으로 주입해야 합니다.
     */
    @Deprecated
    public AdminOverviewService(
            TimeProvider timeProvider,
            AdminCouponRoundDataReader couponRoundDataReader,
            RuntimeConfigStore runtimeConfigStore,
            OverviewCalculationPolicy policy,
            OverviewObservationSource observationSource,
            AdminQueueObservationSource queueObservationSource,
            IssuanceFlowCalculator issuanceFlowCalculator,
            IssuanceActionCalculator issuanceActionCalculator,
            CouponRoundQueueCalculator couponRoundQueueCalculator,
            CustomerOutcomeCalculator customerOutcomeCalculator,
            StockRiskCalculator stockRiskCalculator,
            CouponRoundOverviewCalculator couponRoundOverviewCalculator,
            ConsistencyFinalReader consistencyFinalReader,
            ConsistencyActionCalculator consistencyActionCalculator,
            OperationActionCalculator operationActionCalculator,
            OverviewStatusCalculator overviewStatusCalculator
    ) {
        this(timeProvider, couponRoundDataReader, runtimeConfigStore, policy, observationSource, queueObservationSource,
                issuanceFlowCalculator, issuanceActionCalculator, couponRoundQueueCalculator,
                customerOutcomeCalculator, stockRiskCalculator, couponRoundOverviewCalculator,
                new CouponRoundPreparationCalculator(), consistencyFinalReader, consistencyActionCalculator,
                operationActionCalculator, overviewStatusCalculator);
    }

    /**
     * 현재 시점의 관리자 운영현황을 반환합니다.
     *
     * <p>기준 시각, DB catalog, Runtime 현재값과 관측 묶음을 각각 한 번 읽습니다. DB couponId 모집단을
     * O1 target과 쿠폰 회차 행에 함께 사용하며, 진행 쿠폰 회차의 O2는 공통 대기열 원천에서 읽습니다.
     * O1·O2·준비·완전한 FINAL 후보의 대표 Action 계산은 한 번만 수행합니다. 불완전 FINAL의 기여는
     * 기존 조치 값을 가리지 않도록 보류하고 O3·전체 발급률·지연은 관측 묶음의 상태와 시각을 그대로
     * 전달합니다.</p>
     *
     * @return Snapshot과 전체 데이터 완전성을 포함한 운영현황 Service 결과
     */
    public AdminOverviewResult getOverview() {
        Instant snapshotAt = timeProvider.instant();
        AdminCouponRoundCatalog dbCatalog = couponRoundDataReader.loadCatalog(snapshotAt);
        // 준비 비교값은 Redis 정본 재고로 치환하기 전 DB total·설정 snapshot에서 만들어야 합니다.
        Map<Long, V2PreparationSource> v2Preparation =
                preparationResolver.resolve(dbCatalog, snapshotAt);
        AdminCouponRoundCatalog catalog = stockResolver.resolve(dbCatalog, snapshotAt);
        // 현재값과 last-known-good를 섞지 않도록 요청마다 get() 결과 하나만 사용합니다.
        RuntimeConfigSnapshot runtimeConfig = runtimeConfigStore.get();
        List<CouponRoundOverviewSource> couponRounds = couponRounds(
                catalog, runtimeConfig, couponRoundPreparationCalculator, v2Preparation);
        List<Long> couponIds = catalog.couponRounds().stream()
                .map(AdminCouponRoundCatalog.CouponRoundData::couponId)
                .toList();
        Map<Long, ConsistencyFinalObservation> finalObservations =
                consistencyFinalReader.findLatestByCouponIds(couponIds);
        requireSameFinalPopulation(couponIds, finalObservations);
        FinalActionCalculation finalCalculation =
                consistencyActionCalculator.calculateLatest(finalObservations);
        List<AdminOverviewSnapshot.Observation<?>> finalActionObservations = new ArrayList<>();
        if (finalCalculation.isComplete()) {
            // 전용 상태 필드가 없으므로 완전한 FINAL만 기존 Action 원천 시각 합성에 포함합니다.
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
        QueueCalculation queueCalculation = couponRoundQueueCalculator.calculate(
                policy, queueInputs(couponRounds, snapshotAt));
        OutcomeCalculation outcomeCalculation = customerOutcomeCalculator.calculate(observationData.outcomeInput());
        StockRiskCalculation stockCalculation = stockRiskCalculator.calculate(
                policy, stockInputs(couponRounds, issuanceCalculation.issuanceFlows()));
        PreparationCalculation preparationCalculation = couponRoundOverviewCalculator.calculatePreparation(
                snapshotAt, couponRounds);
        boolean catalogAvailable = catalog.status() == SourceStatus.VALID;
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.OpeningSoonSummary> preparationObservation =
                catalogAvailable
                        ? preparationCalculation.openingSoon()
                        : emptyObservation(catalog.status());
        // 준비 확정 후보도 O1·O2와 같은 대표 조치 모집단에서 한 번만 선택합니다.
        List<AdminOverviewSnapshot.OperationActionItem> actionCandidates = actionCandidates(
                couponRounds, issuanceActionCandidates, queueCalculation.actionCandidates(),
                preparationCalculation.actionCandidates(),
                finalCalculation.isComplete() ? finalCalculation.actionCandidates() : List.of());
        ActionCalculation actionCalculation = operationActionCalculator.calculate(actionCandidates);
        CouponRoundCalculation couponRoundCalculation = couponRoundOverviewCalculator.calculate(
                snapshotAt, couponRounds, issuanceCalculation.issuanceFlows(),
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
                        ? validObservation(couponRoundCalculation.couponRoundStatusSummary(), catalog.observedAt())
                        : emptyObservation(catalog.status()),
                actionObservation(actionCalculation.items(), queueCalculation.queueRisk(),
                        issuanceCalculation.issuanceFlows(), preparationObservation,
                        finalActionObservations, snapshotAt),
                catalogAvailable
                        ? validObservation(couponRoundCalculation.couponRounds(), catalog.observedAt())
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

    /** DB 쿠폰 회차 모집단을 같은 Snapshot의 기술 중립 관측 요청으로 변환합니다. */
    private OverviewObservationRequest observationRequest(
            Instant snapshotAt,
            AdminCouponRoundCatalog catalog
    ) {
        List<CouponRoundObservationTarget> targets = catalog.couponRounds().stream()
                .map(AdminOverviewService::observationTarget)
                .toList();
        return new OverviewObservationRequest(snapshotAt, targets, policy);
    }

    /** DB 쿠폰 회차 ID와 재고 상태를 변경 없이 O1 관측 target으로 변환합니다. */
    private static CouponRoundObservationTarget observationTarget(AdminCouponRoundCatalog.CouponRoundData couponRound) {
        Boolean stockAvailable = null;
        SourceStatus stockStatus = SourceStatus.N_A;
        if (couponRound.status() == CouponRoundStatus.OPEN) {
            stockStatus = couponRound.stock().status();
        }
        if (couponRound.status() == CouponRoundStatus.OPEN && stockStatus.carriesValue()) {
            stockAvailable = couponRound.stock().value().activeCount()
                    < couponRound.stock().value().totalQuantity();
        }
        return new CouponRoundObservationTarget(
                couponRound.couponId(), couponRound.status(), stockAvailable, stockStatus,
                stockStatus.carriesValue() ? couponRound.stock().observedAt() : null);
    }

    /** DB catalog를 Core가 확정한 준비 결과와 함께 기존 계산기 입력으로 변환합니다. */
    private static List<CouponRoundOverviewSource> couponRounds(
            AdminCouponRoundCatalog catalog,
            RuntimeConfigSnapshot runtimeConfig,
            CouponRoundPreparationCalculator couponRoundPreparationCalculator,
            Map<Long, V2PreparationSource> v2Preparation
    ) {
        if (catalog.status() != SourceStatus.VALID) {
            return List.of();
        }
        return catalog.couponRounds().stream()
                .map(couponRound -> couponRoundSource(
                        couponRound, runtimeConfig, couponRoundPreparationCalculator, v2Preparation))
                .toList();
    }

    /** DB 메타·회차 엔진·권위 재고와 V2 준비 판정을 한 쿠폰 회차 계산 원천으로 조립합니다. */
    private static CouponRoundOverviewSource couponRoundSource(
            AdminCouponRoundCatalog.CouponRoundData couponRound,
            RuntimeConfigSnapshot runtimeConfig,
            CouponRoundPreparationCalculator couponRoundPreparationCalculator,
            Map<Long, V2PreparationSource> v2Preparation
    ) {
        SourceStatus stockStatus = couponRound.status() == CouponRoundStatus.OPEN
                ? couponRound.stock().status() : SourceStatus.N_A;
        if (couponRound.status() == CouponRoundStatus.OPEN && !runtimeConfig.status().carriesValue()) {
            stockStatus = SourceStatus.UNAVAILABLE;
        }
        boolean carriesStock = stockStatus.carriesValue();
        return new CouponRoundOverviewSource(
                couponRound.couponId(), couponRound.couponName(), couponRound.brandName(), couponRound.status(),
                couponRound.opensAt(), couponRound.closesAt(), couponRound.engineVersion(),
                carriesStock ? couponRound.stock().value().totalQuantity() : null,
                carriesStock ? couponRound.stock().value().activeCount() : null,
                carriesStock ? couponRound.stock().observedAt() : null,
                stockStatus,
                couponRoundPreparationCalculator.calculate(
                        couponRound.preparation(), couponRound.engineVersion(),
                        v2Preparation.getOrDefault(
                                couponRound.couponId(), V2PreparationSource.notApplicable())));
    }

    /** OPEN 모집단만 1분 대기열 원천으로 읽고, 비 OPEN 쿠폰 회차는 질의 없이 N_A 입력으로 만듭니다. */
    private List<CouponRoundQueueCalculator.QueueInput> queueInputs(
            List<CouponRoundOverviewSource> couponRounds,
            Instant snapshotAt
    ) {
        List<Long> openCouponIds = couponRounds.stream()
                .filter(couponRound -> couponRound.status() == CouponRoundStatus.OPEN)
                .map(CouponRoundOverviewSource::couponId)
                .toList();
        Map<Long, CouponRoundQueueObservation> observations = Map.of();
        if (!openCouponIds.isEmpty()) {
            Instant windowStart = snapshotAt.minusSeconds(60L);
            observations = queueObservationSource.observe(openCouponIds, windowStart, snapshotAt, snapshotAt);
            requireSameQueuePopulation(openCouponIds, observations);
        }
        Map<Long, CouponRoundQueueObservation> queueObservations = observations;
        return couponRounds.stream()
                .map(couponRound -> queueInput(couponRound, queueObservations.get(couponRound.couponId())))
                .toList();
    }

    /** 원천 부분 응답을 정상 전체 KPI로 사용하지 않도록 OPEN 모집단의 키 일치를 확인합니다. */
    private static void requireSameQueuePopulation(
            List<Long> requestedIds,
            Map<Long, CouponRoundQueueObservation> observations
    ) {
        Set<Long> requested = new LinkedHashSet<>(requestedIds);
        if (observations == null || observations.size() != requested.size()
                || !requested.equals(observations.keySet())) {
            throw new BusinessException(
                    AdminOverviewErrorCode.OBSERVATION_REQUEST_MISMATCH,
                    "대기열 관측 응답이 현재 OPEN 쿠폰 회차 모집단과 일치해야 합니다.");
        }
    }

    /** 공통 대기열 관측을 기존 O2 계산기의 손실 없는 입력으로 변환합니다. */
    private static CouponRoundQueueCalculator.QueueInput queueInput(
            CouponRoundOverviewSource couponRound,
            CouponRoundQueueObservation observation
    ) {
        if (couponRound.status() != CouponRoundStatus.OPEN) {
            // 예약·종료 쿠폰 회차는 O2 적용 대상이 아니므로 원천을 조회하거나 값을 싣지 않습니다.
            return new CouponRoundQueueCalculator.QueueInput(
                    couponRound.couponId(), null, null, null, null, null,
                    null, null, SourceStatus.N_A, null);
        }
        if (observation == null || !couponRound.couponId().equals(observation.couponId())) {
            throw new BusinessException(
                    AdminOverviewErrorCode.OBSERVATION_REQUEST_MISMATCH,
                    "대기열 관측 couponId가 현재 OPEN 쿠폰 회차와 일치해야 합니다.");
        }
        return new CouponRoundQueueCalculator.QueueInput(
                observation.couponId(), observation.currentWaitingCount(), observation.previousWaitingCount(),
                observation.admittedCount(), observation.windowStart(), observation.windowEnd(),
                observation.lastAdmissionAt(), observation.admissionStoppedStartedAt(),
                observation.sourceStatus(), observation.observedAt());
    }

    /**
     * DB 재고와 같은 요청의 O1 결과를 O4 입력으로 변환합니다.
     *
     * <p>예약·종료 쿠폰 회차는 O1이 N_A이므로 O4도 N_A로 명시해 전역 위험 모집단에서 제외합니다.
     * 그 밖의 쿠폰 회차는 CouponRoundOverviewSource가
     * 검증한 명시 재고 상태와 수량·관측 시각을 손실 없이 전달하며, 값 없는 상태의 수량을 0으로
     * 보정하지 않습니다.</p>
     */
    private static List<StockInput> stockInputs(
            List<CouponRoundOverviewSource> couponRounds,
            Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows
    ) {
        List<StockInput> inputs = new ArrayList<>();
        for (CouponRoundOverviewSource couponRound : couponRounds) {
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> issuanceFlow =
                    issuanceFlows.get(couponRound.couponId());
            SourceStatus stockStatus = issuanceFlow != null && issuanceFlow.status() == SourceStatus.N_A
                    ? SourceStatus.N_A : couponRound.stockStatus();
            // 값 없는 상태에는 관측 시각을 싣지 않는 공통 Observation 불변식을 지킵니다.
            Instant observedAt = stockStatus.carriesValue() ? couponRound.stockObservedAt() : null;
            inputs.add(new StockInput(couponRound.couponId(), couponRound.engineVersion(), couponRound.totalQuantity(),
                    couponRound.activeCount(), stockStatus, observedAt, issuanceFlow));
        }
        return List.copyOf(inputs);
    }

    /**
     * O1·O2·준비·FINAL 후보를 한 Action 계산 호출의 같은 모집단으로 결합합니다.
     *
     * <p>O1·O2 계산기는 기술 중립성을 위해 이름·오픈 시각 없이 후보를 만들 수 있으므로, 이 조립 경계에서
     * 같은 couponId의 기존 쿠폰 회차 기본 정보만 채웁니다. 정책에서 확정한 심각도·영향·권장 행동은 바꾸지
     * 않으며, 준비 후보도 같은 전체 모집단에 넣고 목록의 상위 20개를 여기서 참조하지 않습니다.</p>
     */
    private static List<AdminOverviewSnapshot.OperationActionItem> actionCandidates(
            List<CouponRoundOverviewSource> couponRounds,
            List<AdminOverviewSnapshot.OperationActionItem> issuanceCandidates,
            List<AdminOverviewSnapshot.OperationActionItem> queueCandidates,
            List<AdminOverviewSnapshot.OperationActionItem> preparationCandidates,
            List<AdminOverviewSnapshot.OperationActionItem> consistencyCandidates
    ) {
        Map<Long, CouponRoundOverviewSource> couponRoundByCoupon = couponRounds.stream()
                .collect(java.util.stream.Collectors.toMap(CouponRoundOverviewSource::couponId, couponRound -> couponRound));
        List<AdminOverviewSnapshot.OperationActionItem> candidates = new ArrayList<>();
        candidates.addAll(withCouponRoundDisplay(issuanceCandidates, couponRoundByCoupon));
        candidates.addAll(withCouponRoundDisplay(queueCandidates, couponRoundByCoupon));
        // DB에서 확정한 준비 후보를 O1·O2 후보와 같은 대표 판정에 포함합니다.
        candidates.addAll(preparationCandidates);
        // FINAL 저장 당시 표시값 대신 같은 Overview 요청의 현재 쿠폰 회차 표시값을 사용합니다.
        candidates.addAll(withCouponRoundDisplay(consistencyCandidates, couponRoundByCoupon));
        return List.copyOf(candidates);
    }

    /** 기술 중립 O1·O2 후보에 동일 couponId의 화면 표시용 쿠폰 회차 정보를 보강합니다. */
    private static List<AdminOverviewSnapshot.OperationActionItem> withCouponRoundDisplay(
            List<AdminOverviewSnapshot.OperationActionItem> candidates,
            Map<Long, CouponRoundOverviewSource> couponRoundByCoupon
    ) {
        List<AdminOverviewSnapshot.OperationActionItem> displayedCandidates = new ArrayList<>();
        for (AdminOverviewSnapshot.OperationActionItem candidate : candidates) {
            CouponRoundOverviewSource couponRound = couponRoundByCoupon.get(candidate.couponId());
            // 기술 중립 후보에 화면 표시용 쿠폰 회차 이름과 오픈 시각만 보강합니다.
            displayedCandidates.add(new AdminOverviewSnapshot.OperationActionItem(candidate.couponId(),
                    couponRound == null ? candidate.couponName() : couponRound.couponName(),
                    couponRound == null ? candidate.opensAt() : couponRound.opensAt(), candidate.severity(),
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
     * @param snapshot 쿠폰 회차·관측 원천별 계산이 끝난 기술 중립 결과
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

    /** O1·O2·준비와 완전한 FINAL 모집단을 Action KPI·목록 상태에 반영합니다. */
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
        // 준비 여부를 모르는 쿠폰 회차가 있으면 빈 정상 조치 목록으로 보정하지 않습니다.
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

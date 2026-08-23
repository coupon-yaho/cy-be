package com.kafkick.api.admin.dashboard.dto;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import com.kafkick.core.admin.overview.AdminOverviewResult.OverallStatus;
import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.ActionCode;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.CampaignQueueAssessment;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.CustomerImpact;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.CustomerOutcomeType;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.IssuanceFlowState;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.TargetScreen;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot.TrendDirection;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.coupon.CouponStatus;

/**
 * 관리자 첫 화면에 표시할 운영 위험과 조치 항목을 한 시점 기준으로 조립한 HTTP 응답 초안입니다.
 *
 * <p>{@code overallStatus}는 전체 응답의 완전성({@code COMPLETE/PARTIAL/UNAVAILABLE})을 나타내고,
 * 각 {@link ObservedValue}의 상태는 개별 원천의 수집 상태를 나타냅니다. 수집하지 못한 값을 정상 또는 0으로
 * 위조하지 않으며 value를 null로 유지합니다. 완전성·심각도·조치 유형·고객 영향·대상 화면은
 * 명시적 enum으로 고정해 오타나 임의 코드를 허용하지 않습니다.</p>
 *
 * <p>구체 {@code AdminOverviewService}가 캠페인 Repository와 관측 조회 결과를
 * {@code AdminOverviewResult}로 조립하면 Controller가 이 DTO의 정적 팩토리로 응답을 생성합니다.
 * 별도 Provider·Service 인터페이스·Mapper 계층을 두지 않으며, DB·Redis·Kafka의 원시 기술 값은
 * HTTP 응답에 직접 노출하지 않습니다.</p>
 *
 * @param snapshotAt 이 응답이 나타내는 기준 시각
 * @param overallStatus 전체 응답 데이터의 완전성 상태
 * @param actionRequired 조치 필요 캠페인의 전체·긴급·주의 수와 해당 원천 상태
 * @param openingSoon 30분 내 오픈 및 준비 미완료 캠페인 수와 해당 원천 상태
 * @param queueRisk 대기열 기준 초과 요약과 해당 원천 상태
 * @param stockRisk 재고·소진 위험 요약과 해당 원천 상태
 * @param aggregateIssuanceRate 전체 캠페인의 현재·최고 발급률과 관측 상태
 * @param aggregateQueue 전체 캠페인의 대기 인원·처리율·예상 대기시간과 관측 상태
 * @param latencySummary 성공·실패 응답 p99와 관측 구간 및 관측 상태
 * @param campaignStatusSummary 진행·예정·종료 캠페인 수와 관측 상태
 * @param actionItems 조치 항목 요약과 해당 원천 상태
 * @param campaigns 캠페인 기본 목록과 O1·O2·O4 중첩 관측값; 바깥 상태는 기본 목록 조회 상태
 * @param customerOutcomes 최근 관측 구간의 전 캠페인 O3 고객 결과 집계
 */
public record AdminOverviewResponse(
        Instant snapshotAt, OverallStatus overallStatus,
        ObservedValue<ActionRequiredSummary> actionRequired,
        ObservedValue<OpeningSoonSummary> openingSoon,
        ObservedValue<QueueRiskSummary> queueRisk, ObservedValue<StockRiskSummary> stockRisk,
        ObservedValue<AggregateIssuanceRate> aggregateIssuanceRate,
        ObservedValue<AggregateQueue> aggregateQueue,
        ObservedValue<LatencySummary> latencySummary,
        ObservedValue<CampaignStatusSummary> campaignStatusSummary,
        ObservedValue<ActionItemSummary> actionItems,
        ObservedValue<List<CampaignOverview>> campaigns,
        ObservedValue<CustomerOutcomeSummary> customerOutcomes) {

    /**
     * Controller가 전달한 Service 계산 결과를 HTTP 응답으로 변환합니다.
     *
     * <p>이 메서드는 위험도, KPI, 전체 완전성을 다시 계산하지 않습니다. Service가 확정한
     * {@code overallStatus}와 Core 값들을 그대로 보존하면서 API 전용 record로 옮기는 역할만 합니다.
     * 별도 Mapper 계층 없이 DTO가 자신의 변환 규칙을 소유하되, 원천 조회나 운영 정책은 DTO에
     * 들어오지 않도록 경계를 유지합니다.</p>
     *
     * @param snapshot Service 결과에 포함된 운영현황 Snapshot
     * @param overallStatus Service 결과에 포함된 전체 완전성
     * @return 값·원천 상태·관측 시각을 보존한 HTTP 응답
     */
    public static AdminOverviewResponse from(
            AdminOverviewSnapshot snapshot,
            OverallStatus overallStatus
    ) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(overallStatus, "overallStatus");
        return new AdminOverviewResponse(
                snapshot.snapshotAt(),
                overallStatus,
                fromObservation(snapshot.actionRequired(), AdminOverviewResponse::toActionRequiredSummary),
                fromObservation(snapshot.openingSoon(), AdminOverviewResponse::toOpeningSoonSummary),
                fromObservation(snapshot.queueRisk(), AdminOverviewResponse::toQueueRiskSummary),
                fromObservation(snapshot.stockRisk(), AdminOverviewResponse::toStockRiskSummary),
                fromObservation(
                        snapshot.aggregateIssuanceRate(),
                        AdminOverviewResponse::toAggregateIssuanceRate),
                fromObservation(snapshot.aggregateQueue(), AdminOverviewResponse::toAggregateQueue),
                fromObservation(snapshot.latencySummary(), AdminOverviewResponse::toLatencySummary),
                fromObservation(
                        snapshot.campaignStatusSummary(),
                        AdminOverviewResponse::toCampaignStatusSummary),
                fromObservation(snapshot.actionItems(), AdminOverviewResponse::toActionItemSummary),
                fromObservation(snapshot.campaigns(), AdminOverviewResponse::toCampaignOverviews),
                fromObservation(
                        snapshot.customerOutcomes(),
                        AdminOverviewResponse::toCustomerOutcomeSummary)
        );
    }

    /** 실제 관측값과 시각이 없는 독립 원천을 공통 계약에 맞춰 생성합니다. */
    private static <T> ObservedValue<T> unavailableValue() {
        return new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null);
    }

    /**
     * Core 관측 Wrapper의 값·상태·관측 시각을 API 관측 Wrapper로 옮깁니다.
     *
     * <p>값이 없는 상태에서는 변환기를 호출하지 않으므로 {@code UNAVAILABLE}, {@code PENDING},
     * {@code N_A}의 null 의미가 유지됩니다. Snapshot 조립 과정에서 관측 영역 자체가 누락된 경우도
     * 미수집으로 표현해 응답 조립이 가짜 정상값을 만들지 않도록 합니다.</p>
     */
    private static <S, T> ObservedValue<T> fromObservation(
            AdminOverviewSnapshot.Observation<S> source,
            Function<S, T> converter
    ) {
        if (source == null) {
            return unavailableValue();
        }
        T value = source.value() == null ? null : converter.apply(source.value());
        return new ObservedValue<>(value, source.status(), source.observedAt());
    }

    private static ActionRequiredSummary toActionRequiredSummary(
            AdminOverviewSnapshot.ActionRequiredSummary source
    ) {
        return new ActionRequiredSummary(
                source.totalCount(), source.urgentCount(), source.warningCount());
    }

    private static OpeningSoonSummary toOpeningSoonSummary(
            AdminOverviewSnapshot.OpeningSoonSummary source
    ) {
        return new OpeningSoonSummary(source.totalCount(), source.preparationIncompleteCount());
    }

    private static QueueRiskSummary toQueueRiskSummary(AdminOverviewSnapshot.QueueRiskSummary source) {
        return new QueueRiskSummary(source.thresholdExceededCount(), source.longestWait());
    }

    private static StockRiskSummary toStockRiskSummary(AdminOverviewSnapshot.StockRiskSummary source) {
        return new StockRiskSummary(source.depletionRiskCount(), source.nearestDepletion());
    }

    private static AggregateIssuanceRate toAggregateIssuanceRate(
            AdminOverviewSnapshot.AggregateIssuanceRate source
    ) {
        return new AggregateIssuanceRate(source.currentPerSecond(), source.sessionPeakPerSecond());
    }

    private static AggregateQueue toAggregateQueue(AdminOverviewSnapshot.AggregateQueue source) {
        return new AggregateQueue(
                source.waitingCount(), source.admissionsPerSecond(), source.estimatedWait());
    }

    private static LatencySummary toLatencySummary(AdminOverviewSnapshot.LatencySummary source) {
        return new LatencySummary(
                source.successfulP99(), source.failedP99(), source.windowStart(), source.windowEnd());
    }

    private static CampaignStatusSummary toCampaignStatusSummary(
            AdminOverviewSnapshot.CampaignStatusSummary source
    ) {
        return new CampaignStatusSummary(
                source.openCount(), source.scheduledCount(), source.closedCount());
    }

    private static ActionItemSummary toActionItemSummary(
            AdminOverviewSnapshot.ActionItemSnapshot source
    ) {
        return new ActionItemSummary(
                source.totalCount(),
                source.topItems().stream()
                        .map(AdminOverviewResponse::toOperationActionItem)
                        .toList());
    }

    private static List<CampaignOverview> toCampaignOverviews(
            List<AdminOverviewSnapshot.CampaignOverview> source
    ) {
        return source.stream().map(AdminOverviewResponse::toCampaignOverview).toList();
    }

    private static CampaignOverview toCampaignOverview(
            AdminOverviewSnapshot.CampaignOverview source
    ) {
        return new CampaignOverview(
                source.priority(),
                source.couponId(),
                source.campaignName(),
                source.brandName(),
                source.status(),
                source.opensAt(),
                source.closesAt(),
                source.severity(),
                fromObservation(source.issuanceFlow(), AdminOverviewResponse::toIssuanceFlow),
                fromObservation(
                        source.campaignQueueStatus(),
                        AdminOverviewResponse::toCampaignQueueStatus),
                fromObservation(source.stockForecast(), AdminOverviewResponse::toStockForecast),
                source.customerImpact(),
                source.customerImpactText(),
                toRecommendedAction(source.recommendedAction())
        );
    }

    private static IssuanceFlow toIssuanceFlow(AdminOverviewSnapshot.IssuanceFlow source) {
        return new IssuanceFlow(
                source.currentPerMinute(),
                source.windowStart(),
                source.windowEnd(),
                source.points().stream()
                        .map(AdminOverviewResponse::toIssuanceRatePoint)
                        .toList(),
                source.state(),
                source.stateDuration());
    }

    private static IssuanceRatePoint toIssuanceRatePoint(
            AdminOverviewSnapshot.IssuanceRatePoint source
    ) {
        return new IssuanceRatePoint(source.observedAt(), source.issuancesPerMinute());
    }

    private static CampaignQueueStatus toCampaignQueueStatus(
            AdminOverviewSnapshot.CampaignQueueStatus source
    ) {
        return new CampaignQueueStatus(
                source.waitingCount(),
                source.trend(),
                source.waitingDeltaPerMinute(),
                source.admissionsPerMinute(),
                source.estimatedWait(),
                source.assessment());
    }

    private static StockForecast toStockForecast(AdminOverviewSnapshot.StockForecast source) {
        return new StockForecast(
                source.remainingQuantity(),
                source.totalQuantity(),
                source.remainingRatio(),
                source.estimatedDepletion());
    }

    private static CustomerOutcomeSummary toCustomerOutcomeSummary(
            AdminOverviewSnapshot.CustomerOutcomeSummary source
    ) {
        return new CustomerOutcomeSummary(
                source.windowStart(),
                source.windowEnd(),
                source.totalCount(),
                source.outcomes().stream()
                        .map(AdminOverviewResponse::toCustomerOutcome)
                        .toList());
    }

    private static CustomerOutcome toCustomerOutcome(AdminOverviewSnapshot.CustomerOutcome source) {
        return new CustomerOutcome(
                source.type(), source.count(), source.ratio(), source.displayText());
    }

    private static OperationActionItem toOperationActionItem(
            AdminOverviewSnapshot.OperationActionItem source
    ) {
        return new OperationActionItem(
                source.couponId(),
                source.campaignName(),
                source.opensAt(),
                source.severity(),
                source.customerImpact(),
                source.customerImpactText(),
                source.detectedAt(),
                source.duration(),
                toRecommendedAction(source.recommendedAction()));
    }

    private static RecommendedAction toRecommendedAction(
            AdminOverviewSnapshot.RecommendedAction source
    ) {
        if (source == null) {
            return null;
        }
        return new RecommendedAction(source.code(), source.displayText(), source.targetScreen());
    }

    /**
     * 조치가 필요한 캠페인의 전체·긴급·주의 건수를 구분한 요약입니다.
     *
     * @param totalCount 전체 조치 필요 캠페인 수
     * @param urgentCount 즉시 조치가 필요한 캠페인 수
     * @param warningCount 주의 수준 캠페인 수
     */
    public record ActionRequiredSummary(long totalCount, long urgentCount, long warningCount) { }

    /**
     * 30분 안에 오픈하는 캠페인과 준비 미완료 캠페인 수를 구분한 요약입니다.
     *
     * @param totalCount 30분 안에 오픈하는 전체 캠페인 수
     * @param preparationIncompleteCount 그중 준비 완료가 확인되지 않은 캠페인 수
     */
    public record OpeningSoonSummary(long totalCount, long preparationIncompleteCount) { }

    /**
     * 대기열 기준을 초과한 것으로 판정된 캠페인 수입니다.
     *
     * <p>{@code longestWait}는 단위를 이름으로 추측하지 않도록 {@link Duration}으로 전달합니다.
     * 원천이 미관측 상태이거나 대기 시작 시각을 알 수 없어 계산할 수 없으면 null입니다.</p>
     *
     * @param thresholdExceededCount 대기 인원·시간 기준을 초과한 캠페인 수
     * @param longestWait 가장 긴 대기시간; 미관측 또는 계산 불가이면 null
     */
    public record QueueRiskSummary(long thresholdExceededCount, Duration longestWait) { }

    /**
     * DB 재고와 소진 예상 규칙에서 위험으로 판정한 캠페인 수입니다.
     *
     * <p>{@code nearestDepletion}은 실제 발급률로 예측할 수 있을 때만 제공하며 미관측, 무트래픽 또는
     * 예측 불가 상태에서는 null입니다. null을 0으로 바꾸면 ‘즉시 소진’으로 오해되므로 그대로 유지합니다.</p>
     *
     * @param depletionRiskCount 재고 부족 또는 소진 임박 캠페인 수
     * @param nearestDepletion 가장 가까운 예상 소진까지의 시간; 계산할 수 없으면 null
     */
    public record StockRiskSummary(long depletionRiskCount, Duration nearestDepletion) { }

    /**
     * 전체 캠페인 발급 속도 카드에 표시할 초당 성공 발급률입니다.
     *
     * @param currentPerSecond 실제 경과시간으로 보정한 현재 초당 성공 발급 건수
     * @param sessionPeakPerSecond 현재 화면 관측 세션의 최고 초당 성공 발급 건수
     */
    public record AggregateIssuanceRate(double currentPerSecond, double sessionPeakPerSecond) { }

    /**
     * 전체 캠페인 대기열 카드에 표시할 합계와 처리 상태입니다.
     *
     * <p>{@code waitingCount=0}은 실제 대기자가 없다는 뜻입니다. 입장 처리율이 0이라 예상시간을
     * 계산할 수 없으면 {@code estimatedWait=null}이며, 임의로 0초를 반환하지 않습니다.</p>
     *
     * @param waitingCount 현재 전체 대기 인원
     * @param admissionsPerSecond 현재 초당 입장 처리 인원
     * @param estimatedWait 전체 대기 기준 예상시간; 계산할 수 없으면 null
     */
    public record AggregateQueue(long waitingCount, double admissionsPerSecond, Duration estimatedWait) { }

    /**
     * 성공과 실패 요청을 섞지 않은 응답 p99 요약입니다.
     *
     * @param successfulP99 성공 응답 p99; 표본이 없거나 미관측이면 null
     * @param failedP99 실패 응답 p99; 표본이 없거나 미관측이면 null
     * @param windowStart 관측 구간 시작 시각
     * @param windowEnd 관측 구간 종료 시각
     */
    public record LatencySummary(Duration successfulP99, Duration failedP99,
                                 Instant windowStart, Instant windowEnd) { }

    /**
     * 전체 캠페인의 예약·진행·종료 상태별 건수입니다.
     *
     * @param openCount 현재 진행 중인 캠페인 수
     * @param scheduledCount 오픈 예정 캠페인 수
     * @param closedCount 종료된 캠페인 수
     */
    public record CampaignStatusSummary(long openCount, long scheduledCount, long closedCount) { }

    /**
     * 캠페인 운영 상태표와 O1·O2·O4가 공유하는 캠페인 한 행의 HTTP 표현입니다.
     *
     * <p>목록 바깥 {@link ObservedValue}의 상태는 캠페인 기본 목록 조회 상태입니다. 발급 흐름,
     * 대기 상태, 재고 상태는 서로 다른 원천이므로 각 내부 {@link ObservedValue}의 상태와 관측 시각을
     * 독립적으로 사용합니다. 실제 DB·Redis 연결 여부와 무관하게 원천별 상태를 보존합니다.</p>
     *
     * @param priority 서버가 결정한 운영 조치 우선순위
     * @param couponId 캠페인 상세 이동에 사용할 쿠폰 회차 식별자
     * @param campaignName 화면에 표시할 캠페인 이름
     * @param brandName 화면 필터와 표에 표시할 브랜드 이름
     * @param status 캠페인 회차의 {@link CouponStatus}
     * @param opensAt 오픈 시각
     * @param closesAt 예정 종료 시각; 미지정이면 null
     * @param severity 운영 조치 우선순위 심각도
     * @param issuanceFlow O1 캠페인별 발급 흐름과 원천 상태; O4의 최근 분당 발급 속도도
     *                     이 값의 {@link IssuanceFlow#currentPerMinute()}를 사용
     * @param campaignQueueStatus O2 캠페인별 대기 상태와 원천 상태
     * @param stockForecast O4 캠페인별 재고·소진 예상과 원천 상태
     * @param customerImpact 고객 영향 범위
     * @param customerImpactText 운영자에게 표시할 고객 영향 설명
     * @param recommendedAction 서버가 제공하는 다음 행동; 조치 불필요이면 null
     */
    public record CampaignOverview(
            int priority,
            Long couponId,
            String campaignName,
            String brandName,
            CouponStatus status,
            Instant opensAt,
            Instant closesAt,
            Severity severity,
            ObservedValue<IssuanceFlow> issuanceFlow,
            ObservedValue<CampaignQueueStatus> campaignQueueStatus,
            ObservedValue<StockForecast> stockForecast,
            CustomerImpact customerImpact,
            String customerImpactText,
            RecommendedAction recommendedAction) { }

    /**
     * O1 캠페인별 현재 발급 속도와 그래프 시계열입니다.
     *
     * @param currentPerMinute 현재 분당 성공 발급 건수; 실제 무트래픽이면 0
     * @param windowStart 시계열 관측 구간 시작 시각
     * @param windowEnd 시계열 관측 구간 종료 시각
     * @param points 시각별 분당 발급 건수; 관측점이 없으면 빈 목록
     * @param state 서버가 판정한 발급 중단·감소·정상 상태
     * @param stateDuration 현재 상태 지속시간; 계산할 수 없으면 null
     */
    public record IssuanceFlow(double currentPerMinute, Instant windowStart, Instant windowEnd,
                               List<IssuanceRatePoint> points,
                               IssuanceFlowState state, Duration stateDuration) { }

    /**
     * O1 발급 흐름 그래프의 한 관측점입니다.
     *
     * @param observedAt 원천에서 발급률을 관측한 시각
     * @param issuancesPerMinute 해당 시점의 분당 성공 발급 건수
     */
    public record IssuanceRatePoint(Instant observedAt, double issuancesPerMinute) { }

    /**
     * O2 캠페인별 대기 상태의 HTTP 표현입니다.
     *
     * <p>기존 core의 {@code QueueState} 수명주기 enum과 의미가 다르므로 이름을
     * {@code CampaignQueueStatus}로 구분합니다.</p>
     *
     * @param waitingCount 현재 대기 인원; 실제 대기자가 없으면 0
     * @param trend 대기 인원의 최근 변화 방향
     * @param waitingDeltaPerMinute 분당 대기 인원 증감; 감소는 음수
     * @param admissionsPerMinute 분당 입장 처리 인원; 적용되지 않으면 null
     * @param estimatedWait 예상 대기시간; 처리율 0 또는 계산 불가이면 null
     * @param assessment 서버가 판정한 대기 운영 상태
     */
    public record CampaignQueueStatus(
            long waitingCount,
            TrendDirection trend,
            long waitingDeltaPerMinute,
            Double admissionsPerMinute,
            Duration estimatedWait,
            CampaignQueueAssessment assessment) { }

    /**
     * O4 캠페인별 잔여 재고와 예상 소진 상태입니다.
     *
     * <p>V1은 MySQL, V2·V3는 Redis에서 재고를 읽지만 HTTP 계약에는 원천 기술을 노출하지 않습니다.
     * O4의 최근 분당 발급 속도는 같은 {@link CampaignOverview}의
     * {@link CampaignOverview#issuanceFlow()}에서 읽습니다. 발급 속도를 이 record에 중복하지 않아
     * 재고와 발급 원천의 {@link SourceStatus} 및 관측 시각을 서로 독립적으로 보존합니다.</p>
     *
     * @param remainingQuantity 현재 잔여 수량; 실제 소진이면 0
     * @param totalQuantity 전체 발급 가능 수량
     * @param remainingRatio 잔여 비율 0~1
     * @param estimatedDepletion 예상 소진까지의 시간; 계산 불가이면 null
     */
    public record StockForecast(long remainingQuantity, long totalQuantity,
                                double remainingRatio, Duration estimatedDepletion) { }

    /**
     * O3 최근 관측 구간의 전체 캠페인 고객 결과 집계입니다.
     *
     * @param windowStart 집계 구간 시작 시각
     * @param windowEnd 집계 구간 종료 시각
     * @param totalCount Prometheus가 추정한 결과별 비율의 전체 분모
     * @param outcomes 결과 유형별 건수·비율·표시 설명; 결과가 없으면 빈 목록
     */
    public record CustomerOutcomeSummary(Instant windowStart, Instant windowEnd,
                                         double totalCount, List<CustomerOutcome> outcomes) {

        /** 비유한·음수 추정치와 mutable 목록이 HTTP 응답으로 누출되는 것을 차단합니다. */
        public CustomerOutcomeSummary {
            Objects.requireNonNull(windowStart, "windowStart");
            Objects.requireNonNull(windowEnd, "windowEnd");
            if (!windowEnd.isAfter(windowStart)) {
                throw new IllegalArgumentException("O3 집계 구간은 양수여야 합니다.");
            }
            if (!Double.isFinite(totalCount) || totalCount < 0d) {
                throw new IllegalArgumentException("totalCount는 유한한 0 이상이어야 합니다.");
            }
            outcomes = List.copyOf(Objects.requireNonNull(outcomes, "outcomes"));
            if ((totalCount == 0d) != outcomes.isEmpty()) {
                throw new IllegalArgumentException("totalCount가 0일 때만 outcomes가 비어야 합니다.");
            }
            if (totalCount > 0d) {
                java.util.EnumSet<CustomerOutcomeType> types =
                        java.util.EnumSet.noneOf(CustomerOutcomeType.class);
                double sum = 0d;
                for (CustomerOutcome outcome : outcomes) {
                    if (!types.add(outcome.type())) {
                        throw new IllegalArgumentException("O3 outcome type은 중복될 수 없습니다.");
                    }
                    sum += outcome.count();
                    if (!Double.isFinite(sum)) {
                        throw new IllegalArgumentException("O3 outcome count 합계는 유한해야 합니다.");
                    }
                    if (!equalWithinAccumulationUlps(
                            outcome.ratio(), outcome.count() / totalCount, 1)) {
                        throw new IllegalArgumentException("O3 outcome ratio가 count/totalCount와 맞지 않습니다.");
                    }
                }
                if (!equalWithinAccumulationUlps(sum, totalCount, outcomes.size())) {
                    throw new IllegalArgumentException("O3 outcome count 합이 totalCount와 맞지 않습니다.");
                }
            }
        }

        /** 각 합산 항마다 최대 1 ULP의 반올림 차이만 허용합니다. */
        private static boolean equalWithinAccumulationUlps(
                double left, double right, int termCount
        ) {
            if (left == right) {
                return true;
            }
            double tolerance = Math.max(Math.ulp(left), Math.ulp(right)) * termCount;
            return Math.abs(left - right) <= tolerance;
        }
    }

    /**
     * O3 고객 결과 유형 하나의 집계값입니다.
     *
     * @param type 고객 결과 코드
     * @param count Prometheus가 추정한 해당 결과 발생 건수; 관측된 활동이 없으면 0
     * @param ratio 전체 {@code totalCount} 대비 0~1 비율; NaN과 무한대는 허용하지 않음
     * @param displayText 운영자에게 표시할 결과 의미 설명
     */
    public record CustomerOutcome(CustomerOutcomeType type, double count,
                                  double ratio, String displayText) {

        /**
         * 잘못된 O3 비율이 HTTP 응답으로 직렬화되는 것을 생성 시점에 차단합니다.
         *
         * @throws IllegalArgumentException ratio가 유한하지 않거나 0 미만 또는 1 초과인 경우
         */
        public CustomerOutcome {
            Objects.requireNonNull(type, "type");
            if (!Double.isFinite(count) || count < 0d) {
                throw new IllegalArgumentException("count는 유한한 0 이상이어야 합니다.");
            }
            if (!Double.isFinite(ratio) || ratio < 0.0 || ratio > 1.0) {
                throw new IllegalArgumentException("ratio는 유한한 0 이상 1 이하 값이어야 합니다.");
            }
        }
    }

    /**
     * 전체 조치 건수와 화면에 우선 노출할 상위 20개 항목을 분리합니다.
     *
     * @param totalCount 전체 조치 필요 항목 수
     * @param topItems 심각도·감지 시각 기준으로 우선 노출할 상위 항목
     */
    public record ActionItemSummary(long totalCount, List<OperationActionItem> topItems) { }

    /**
     * 조치가 필요한 캠페인, 고객 영향과 서버 권장 행동을 나타냅니다.
     *
     * @param couponId 조치 대상 쿠폰 캠페인 회차 식별자
     * @param campaignName 운영 화면에 표시할 캠페인 이름
     * @param opensAt 캠페인 오픈 시각; 확인할 수 없으면 null
     * @param severity 위험 심각도
     * @param customerImpact 고객 영향 범위
     * @param customerImpactText 운영자에게 표시할 현재 고객 영향 설명
     * @param detectedAt 위험을 최초 감지한 시각
     * @param duration 위험 지속 시간; 계산할 수 없으면 null
     * @param recommendedAction 서버가 결정한 권장 행동 코드·표시 문구·버튼 목적지
     */
    public record OperationActionItem(Long couponId, String campaignName, Instant opensAt,
                                      Severity severity, CustomerImpact customerImpact,
                                      String customerImpactText, Instant detectedAt,
                                      Duration duration, RecommendedAction recommendedAction) { }

    /**
     * 프론트가 조치 문구와 이동 위치를 재판정하지 않도록 서버 결정을 한 구조로 전달합니다.
     *
     * <p>{@code code}는 분기와 추적에 사용하는 안정적인 값이고 {@code displayText}는 운영자에게 그대로
     * 표시할 문구입니다. {@code targetScreen}은 버튼 목적지를 제한된 enum으로 고정합니다.</p>
     *
     * @param code 서버가 판정한 권장 행동 코드
     * @param displayText 운영자에게 표시할 서버 제공 문구
     * @param targetScreen 권장 행동 버튼이 이동할 관리자 화면
     */
    public record RecommendedAction(ActionCode code, String displayText, TargetScreen targetScreen) { }

}

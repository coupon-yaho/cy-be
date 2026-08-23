package com.kafkick.core.admin.overview;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;
import com.kafkick.core.coupon.CouponStatus;

/**
 * 관리자 운영현황 Service와 순수 Calculator가 조립하는 기술 중립 결과입니다.
 *
 * <p>HTTP 응답 DTO를 그대로 복제하지 않고 상단 위험 KPI, 전체 발급·대기·지연·캠페인 상태 집계,
 * 조치 목록, 캠페인별 O1·O2·O4, 전체 캠페인 O3 결과 집계로 구성합니다. 각 의미 단위는
 * {@link Observation}으로 값과 원천 상태 및 실제 관측 시각을 분리합니다. 따라서 미관측 값을 0이나
 * 정상 상태로 위조하지 않고 {@code value=null}과 적절한 {@link SourceStatus}로 전달할 수 있습니다.</p>
 *
 * <p>이 모델은 DB Entity, Redis 자료구조, Kafka record, Micrometer meter에 직접 의존하지 않습니다.
 * 캠페인 Repository와 관측 조회 구성요소가 만든 의미 값을 이 Snapshot으로 조립한 뒤 HTTP 응답으로
 * 변환합니다. 실제 운영 원천이 연결되기 전에는 값을 0으로 대신하지 않고 각 영역을 명시적인
 * {@code UNAVAILABLE} 상태로 표현합니다.</p>
 *
 * <p>Core의 {@link Observation}은 운영 의미 모델이며 API 모듈의 HTTP {@code ObservedValue}와
 * 의도적으로 분리합니다. 구체 Service가 이 Snapshot을 반환하면 Controller가 DTO의 정적 팩토리를
 * 호출해 상태·값·관측 시각을 보존하며 Core가 API 표현 타입에 의존하지 않게 합니다.</p>
 *
 * @param snapshotAt 여러 원천을 하나의 결과로 조립한 전체 기준 시각
 * @param actionRequired 조치 필요 캠페인의 전체·긴급·주의 수와 해당 관측 상태
 * @param openingSoon 30분 내 오픈 및 준비 미완료 캠페인 수와 해당 관측 상태
 * @param queueRisk 대기 기준 초과 수와 최장 대기시간 및 해당 관측 상태
 * @param stockRisk 소진 위험 수와 가장 가까운 예상 소진시간 및 해당 관측 상태
 * @param aggregateIssuanceRate 전체 캠페인의 현재·세션 최고 발급률과 관측 상태
 * @param aggregateQueue 전체 캠페인의 대기 인원·입장 처리율·예상 대기시간과 관측 상태
 * @param latencySummary 성공·실패 응답 p99와 관측 구간 및 관측 상태
 * @param campaignStatusSummary 진행·예정·종료 캠페인 수와 관측 상태
 * @param actionItems 서버가 판정한 전체 건수와 상위 20개 조치 및 해당 관측 상태
 * @param campaigns 캠페인 기본 목록과 O1·O2·O4 중첩 관측값; 바깥 상태는 기본 목록 조회 상태만 의미
 * @param customerOutcomes 최근 관측 구간의 전체 캠페인 O3 고객 결과 집계와 관측 상태
 */
public record AdminOverviewSnapshot(
        Instant snapshotAt,
        Observation<ActionRequiredSummary> actionRequired,
        Observation<OpeningSoonSummary> openingSoon,
        Observation<QueueRiskSummary> queueRisk,
        Observation<StockRiskSummary> stockRisk,
        Observation<AggregateIssuanceRate> aggregateIssuanceRate,
        Observation<AggregateQueue> aggregateQueue,
        Observation<LatencySummary> latencySummary,
        Observation<CampaignStatusSummary> campaignStatusSummary,
        Observation<ActionItemSnapshot> actionItems,
        Observation<List<CampaignOverview>> campaigns,
        Observation<CustomerOutcomeSummary> customerOutcomes) {

    public AdminOverviewSnapshot {
        if (campaigns != null && campaigns.value() != null) {
            campaigns = new Observation<>(
                    List.copyOf(campaigns.value()), campaigns.status(), campaigns.observedAt());
        }
    }

    /**
     * 의미 있는 값과 그 값을 만든 원천 상태·시각을 분리합니다.
     *
     * <p>{@code value}는 PENDING, UNAVAILABLE, N_A 등 계산할 수 없는 상태에서 null일 수 있습니다.
     * 실제 관측 이력이 없다면 {@code observedAt}도 null이며, 0은 원천에서 실제로 0을 관측했을 때만
     * 사용합니다. {@code observedAt}은 {@link AdminOverviewSnapshot#snapshotAt()}과 달리 해당 원천에서
     * 값을 실제로 관측한 시각입니다.</p>
     *
     * @param <T> 기술 타입이 아닌 운영 의미 값의 타입
     * @param value 실제 관측·계산 값; 미관측 또는 계산 불가이면 null
     * @param status 현재 값의 해석 가능성을 나타내는 공동 SourceStatus 7종 중 하나
     * @param observedAt 원천별 실제 관측 시각; 관측 이력이 없으면 null
     */
    public record Observation<T>(T value, SourceStatus status, Instant observedAt) {

        /**
         * 공통 관측 상태와 동일한 값·시각 규칙을 Snapshot 경계에서 보장합니다.
         *
         * @throws NullPointerException status가 null인 경우
         * @throws IllegalArgumentException 상태와 value 또는 observedAt 조합이 관측 규칙을 위반한 경우
         */
        public Observation {
            Objects.requireNonNull(status, "status");
            switch (status) {
                case VALID, WARMING_UP, STALE, NO_TRAFFIC -> {
                    if (value == null || observedAt == null) {
                        throw new IllegalArgumentException(
                                status + " 상태에는 value와 observedAt이 필요합니다.");
                    }
                }
                case PENDING, UNAVAILABLE, N_A -> {
                    if (value != null || observedAt != null) {
                        throw new IllegalArgumentException(
                                status + " 상태의 value와 observedAt은 null이어야 합니다.");
                    }
                }
            }
        }
    }

    /**
     * 화면의 ‘조치 필요 캠페인’ KPI를 구성하는 서버 판정 결과입니다.
     *
     * @param totalCount 전체 조치 필요 캠페인 수
     * @param urgentCount 긴급 조치 캠페인 수
     * @param warningCount 주의 조치 캠페인 수
     */
    public record ActionRequiredSummary(long totalCount, long urgentCount, long warningCount) { }

    /**
     * 화면의 ‘30분 내 오픈’ KPI를 구성하는 서버 판정 결과입니다.
     *
     * @param totalCount 30분 안에 오픈하는 전체 캠페인 수
     * @param preparationIncompleteCount 그중 준비 완료가 확인되지 않은 캠페인 수
     */
    public record OpeningSoonSummary(long totalCount, long preparationIncompleteCount) { }

    /**
     * 화면의 ‘대기 기준 초과’ KPI를 구성하는 서버 판정 결과입니다.
     *
     * @param thresholdExceededCount 대기 기준을 초과한 캠페인 수
     * @param longestWait 가장 긴 대기시간; 미관측이거나 계산할 수 없으면 null
     */
    public record QueueRiskSummary(long thresholdExceededCount, Duration longestWait) { }

    /**
     * 화면의 ‘소진 임박’ KPI를 구성하는 서버 판정 결과입니다.
     *
     * @param depletionRiskCount 소진 위험으로 판정된 캠페인 수
     * @param nearestDepletion 가장 가까운 예상 소진까지의 시간; 미관측 또는 예측 불가이면 null
     */
    public record StockRiskSummary(long depletionRiskCount, Duration nearestDepletion) { }

    /**
     * 운영 현황 전체 캠페인의 발급 속도 요약입니다.
     *
     * <p>두 값의 단위는 초당 성공 발급 건수입니다. 실제 요청이 없을 때의 0은
     * {@link SourceStatus#NO_TRAFFIC}과 함께 사용하고, 관측할 수 없으면 이 값 전체를 null로 둡니다.</p>
     *
     * @param currentPerSecond 실제 경과시간으로 보정한 현재 초당 성공 발급 건수
     * @param sessionPeakPerSecond 현재 화면 관측 세션에서 기록한 최고 초당 성공 발급 건수
     */
    public record AggregateIssuanceRate(double currentPerSecond, double sessionPeakPerSecond) { }

    /**
     * 운영 현황 전체 캠페인의 대기열 요약입니다.
     *
     * @param waitingCount 현재 대기 중인 전체 인원; 실제 대기자가 없으면 0
     * @param admissionsPerSecond 현재 초당 입장 처리 인원; 실제 처리량이 0이면 0
     * @param estimatedWait 전체 대기 기준 예상 시간; 처리율 0 또는 계산 불가이면 null
     */
    public record AggregateQueue(long waitingCount, double admissionsPerSecond, Duration estimatedWait) { }

    /**
     * 운영 현황에 표시할 성공·실패 요청 지연 요약입니다.
     *
     * <p>p99는 {@link Duration}으로 단위를 보존합니다. 해당 관측 구간에 성공 또는 실패 표본이 없으면
     * 각 p99는 null일 수 있으며, 0ms로 대신하지 않습니다.</p>
     *
     * @param successfulP99 성공 응답의 p99 지연; 표본이 없거나 관측 불가이면 null
     * @param failedP99 실패 응답의 p99 지연; 표본이 없거나 관측 불가이면 null
     * @param windowStart p99 관측 구간 시작 시각
     * @param windowEnd p99 관측 구간 종료 시각
     */
    public record LatencySummary(Duration successfulP99, Duration failedP99,
                                 Instant windowStart, Instant windowEnd) { }

    /**
     * 현재 조립 시각을 기준으로 한 캠페인 회차 상태별 건수입니다.
     *
     * @param openCount 현재 진행 중인 {@link CouponStatus#OPEN} 캠페인 수
     * @param scheduledCount 오픈 예정인 {@link CouponStatus#SCHEDULED} 캠페인 수
     * @param closedCount 종료된 {@link CouponStatus#CLOSED} 캠페인 수
     */
    public record CampaignStatusSummary(long openCount, long scheduledCount, long closedCount) { }

    /**
     * 캠페인 운영 상태표와 O1·O2·O4가 함께 사용하는 캠페인별 계약입니다.
     *
     * <p>{@code campaigns} 바깥 {@link Observation}은 이 기본 목록을 읽은 상태만 나타냅니다.
     * 발급 흐름, 대기열, 재고는 서로 다른 원천에서 관측되므로 각 내부 Observation의 상태와
     * {@code observedAt}을 독립적으로 해석해야 합니다.</p>
     *
     * @param priority 서버가 결정한 운영 조치 우선순위; 1이 가장 먼저 확인할 항목
     * @param couponId 현재 Query 및 관리자 계약에서 사용하는 쿠폰 캠페인 회차 식별자
     * @param campaignName 화면에 표시할 캠페인 이름
     * @param brandName 화면 필터와 표에 표시할 브랜드 이름
     * @param status 캠페인 회차의 예약·진행·종료 상태
     * @param opensAt 캠페인 오픈 시각
     * @param closesAt 예정 종료 시각; 종료 미지정이면 null
     * @param severity 운영 조치 우선순위를 색상으로 표현할 심각도
     * @param issuanceFlow O1 캠페인별 발급 흐름과 해당 원천 상태; O4의 최근 분당 발급 속도도
     *                     이 값의 {@link IssuanceFlow#currentPerMinute()}를 사용
     * @param campaignQueueStatus O2 캠페인별 대기 상태와 해당 원천 상태
     * @param stockForecast O4 캠페인별 재고·소진 예상과 해당 원천 상태
     * @param customerImpact 고객 영향 범위
     * @param customerImpactText 운영 담당자에게 표시할 고객 영향 설명
     * @param recommendedAction 서버가 제공하는 다음 행동; 조치가 필요 없으면 null
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
            Observation<IssuanceFlow> issuanceFlow,
            Observation<CampaignQueueStatus> campaignQueueStatus,
            Observation<StockForecast> stockForecast,
            CustomerImpact customerImpact,
            String customerImpactText,
            RecommendedAction recommendedAction) { }

    /**
     * O1 캠페인별 발급 속도와 최근 추세입니다.
     *
     * @param currentPerMinute 현재 분당 성공 발급 건수; 실제 무트래픽이면 0
     * @param windowStart 시계열 관측 구간 시작 시각
     * @param windowEnd 시계열 관측 구간 종료 시각
     * @param points 화면 그래프에 표시할 시각별 분당 발급 건수; 관측된 점이 없으면 빈 목록
     * @param state 화면에 표시할 발급 중단·감소·정상 판정
     * @param stateDuration 현재 판정이 지속된 0 이상의 시간; 최초 판정 시각을 알 수 없으면 null
     */
    public record IssuanceFlow(double currentPerMinute, Instant windowStart, Instant windowEnd,
                               List<IssuanceRatePoint> points,
                               IssuanceFlowState state, Duration stateDuration) {

        public IssuanceFlow {
            points = List.copyOf(points);
            if (stateDuration != null && stateDuration.isNegative()) {
                throw new IllegalArgumentException("stateDuration은 음수일 수 없습니다.");
            }
        }
    }

    /**
     * O1 발급 흐름 그래프의 한 관측점입니다.
     *
     * @param observedAt 발급률을 관측한 실제 시각
     * @param issuancesPerMinute 해당 시점의 분당 성공 발급 건수
     */
    public record IssuanceRatePoint(Instant observedAt, double issuancesPerMinute) { }

    /**
     * O2 캠페인별 대기 인원과 입장 처리 상태입니다.
     *
     * <p>기존 {@code com.kafkick.core.admin.QueueState} 수명주기 enum과 의미가 다르므로
     * {@code CampaignQueueStatus}라는 이름을 사용합니다.</p>
     *
     * @param waitingCount 현재 대기 인원; 실제 대기자가 없으면 0
     * @param trend 최근 대기 인원 변화 방향
     * @param waitingDeltaPerMinute 분당 대기 인원 증감; 감소는 음수, 변화 없음은 0
     * @param admissionsPerMinute 분당 입장 처리 인원; 처리율이 실제 0이면 0, 적용되지 않으면 null
     * @param estimatedWait 예상 대기시간; 처리율 0 또는 계산 불가이면 null
     * @param assessment 서버가 판정한 정상·입장 중단·안내 기준 초과 상태
     */
    public record CampaignQueueStatus(
            long waitingCount,
            TrendDirection trend,
            long waitingDeltaPerMinute,
            Double admissionsPerMinute,
            Duration estimatedWait,
            CampaignQueueAssessment assessment) { }

    /**
     * O4 캠페인별 재고와 예상 소진 상태입니다.
     *
     * <p>V1은 MySQL 재고, V2·V3는 Redis 재고를 사용하지만 이 계약에는 기술 원천을 노출하지
     * 않습니다. 원천 선택과 실제 계산은 구현체가 담당합니다. O4 화면에서 함께 표시하는 최근
     * 분당 발급 속도는 같은 {@link CampaignOverview}의 {@link CampaignOverview#issuanceFlow()}에 있는
     * {@link IssuanceFlow#currentPerMinute()}를 사용합니다. 재고 원천과 발급 원천의 상태·관측 시각을
     * 독립적으로 유지하기 위해 이 record에 발급 속도를 중복 저장하지 않습니다.</p>
     *
     * @param remainingQuantity 현재 잔여 수량; 실제 소진이면 0
     * @param totalQuantity 캠페인의 전체 발급 가능 수량
     * @param remainingRatio 잔여 비율 0~1; 예를 들어 5%는 0.05
     * @param estimatedDepletion 예상 소진까지의 시간; 발급률 0 또는 예측 불가이면 null
     */
    public record StockForecast(long remainingQuantity, long totalQuantity,
                                double remainingRatio, Duration estimatedDepletion) { }

    /**
     * O3 최근 관측 구간의 전체 캠페인 고객 결과 집계입니다.
     *
     * @param windowStart 집계 구간 시작 시각
     * @param windowEnd 집계 구간 종료 시각
     * @param totalCount Prometheus가 관측 구간에 추정한 모든 결과 event count의 분모
     * @param outcomes 결과 유형별 추정 건수·비율·설명; 결과가 없으면 빈 목록
     */
    public record CustomerOutcomeSummary(Instant windowStart, Instant windowEnd,
                                         double totalCount, List<CustomerOutcome> outcomes) {

        public CustomerOutcomeSummary {
            Objects.requireNonNull(windowStart, "windowStart");
            Objects.requireNonNull(windowEnd, "windowEnd");
            Objects.requireNonNull(outcomes, "outcomes");
            if (!windowEnd.isAfter(windowStart)) {
                throw new IllegalArgumentException("O3 집계 구간은 양수여야 합니다.");
            }
            if (!Double.isFinite(totalCount) || totalCount < 0d) {
                throw new IllegalArgumentException("totalCount는 유한한 비음수여야 합니다.");
            }
            outcomes = List.copyOf(outcomes);
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
     * @param type HTML에서 구분한 고객 결과 코드
     * @param count Prometheus가 관측 구간에 추정한 해당 결과 event count; 실제 발생하지 않았으면 0
     * @param ratio {@link CustomerOutcomeSummary#totalCount()} 대비 비율 0~1; NaN과 무한대는 허용하지 않음
     * @param displayText 운영자에게 표시할 결과 의미 설명
     */
    public record CustomerOutcome(CustomerOutcomeType type, double count,
                                  double ratio, String displayText) {

        /**
         * O3 비율이 확정 범위를 벗어난 상태로 Adapter 경계를 통과하지 못하게 합니다.
         *
         * @throws IllegalArgumentException ratio가 유한하지 않거나 0 미만 또는 1 초과인 경우
         */
        public CustomerOutcome {
            Objects.requireNonNull(type, "type");
            if (!Double.isFinite(count) || count < 0d) {
                throw new IllegalArgumentException("count는 유한한 비음수여야 합니다.");
            }
            if (!Double.isFinite(ratio) || ratio < 0.0 || ratio > 1.0) {
                throw new IllegalArgumentException("ratio는 유한한 0 이상 1 이하 값이어야 합니다.");
            }
        }
    }

    /**
     * 전체 조치 건수와 관리자 첫 화면에 우선 노출할 상위 20개를 함께 전달합니다.
     *
     * <p>상위 항목은 심각도 내림차순, 최초 감지 시각 오름차순(null은 마지막), couponId 오름차순으로
     * 정렬됩니다. couponId는 항목마다 유일해야 하므로 마지막 정렬 키가 항상 결정적인 순서를 만듭니다.
     * 전체 건수와 상위 목록을 분리해 목록이 잘려도 전체 규모를 잃지 않습니다.</p>
     *
     * @param totalCount 전체 조치 필요 항목 수
     * @param topItems 화면에 우선 노출할 최대 20개 항목
     */
    public record ActionItemSnapshot(long totalCount, List<OperationActionItem> topItems) {

        private static final Comparator<OperationActionItem> PRIORITY_ORDER =
                Comparator.comparing(
                                OperationActionItem::severity,
                                Comparator.nullsLast(Comparator.reverseOrder()))
                        .thenComparing(
                                OperationActionItem::detectedAt,
                                Comparator.nullsLast(Comparator.naturalOrder()))
                        .thenComparing(
                                OperationActionItem::couponId,
                                Comparator.nullsLast(Comparator.naturalOrder()));

        public ActionItemSnapshot {
            Objects.requireNonNull(topItems, "topItems");
            if (totalCount < 0) {
                throw new IllegalArgumentException("totalCount는 음수일 수 없습니다.");
            }
            if (topItems.size() > 20) {
                throw new IllegalArgumentException("topItems는 최대 20개입니다.");
            }
            if (totalCount < topItems.size()) {
                throw new IllegalArgumentException("totalCount는 topItems 크기보다 작을 수 없습니다.");
            }
            long distinctCouponCount = topItems.stream()
                    .map(OperationActionItem::couponId)
                    .distinct()
                    .count();
            if (distinctCouponCount != topItems.size()) {
                throw new IllegalArgumentException("topItems에는 동일한 couponId가 중복될 수 없습니다.");
            }
            topItems = topItems.stream().sorted(PRIORITY_ORDER).toList();
        }
    }

    /**
     * 운영자가 확인할 조치 한 건의 서버 판정 결과입니다.
     *
     * <p>식별자는 현재 관리자 계약과 Query에 맞춰 {@code couponId}를 유지합니다. 지속 시간은 단위가
     * 드러나는 {@link Duration}을 사용하고 계산할 수 없으면 null입니다. 권장 행동의 코드·표시 문구·버튼
     * 목적지는 서버가 {@link RecommendedAction}으로 함께 제공하므로 프론트가 문구를 재판정하지 않습니다.</p>
     *
     * @param couponId 조치 대상 쿠폰 캠페인 회차의 필수 식별자
     * @param campaignName 운영 화면에 표시할 캠페인 이름
     * @param opensAt 캠페인 오픈 시각; 오픈 시각이 없거나 확인할 수 없으면 null
     * @param severity 운영 조치 우선순위의 심각도
     * @param customerImpact 고객 영향 범위
     * @param customerImpactText 현재 고객 영향을 설명하는 서버 제공 문구
     * @param detectedAt 위험을 최초 감지한 시각; 최초 시각을 알 수 없으면 null
     * @param duration 위험이 지속된 시간; 계산할 수 없으면 null
     * @param recommendedAction 서버가 제공하는 권장 행동 코드·문구·버튼 목적지
     */
    public record OperationActionItem(
            Long couponId,
            String campaignName,
            Instant opensAt,
            Severity severity,
            CustomerImpact customerImpact,
            String customerImpactText,
            Instant detectedAt,
            Duration duration,
            RecommendedAction recommendedAction) {

        public OperationActionItem {
            Objects.requireNonNull(couponId, "couponId는 필수입니다.");
        }
    }

    /**
     * 프론트가 임의로 조치 문구나 이동 위치를 조립하지 않도록 서버 판정 결과를 묶습니다.
     *
     * @param code 안정적인 권장 행동 코드
     * @param displayText 운영자에게 그대로 표시할 서버 제공 문구
     * @param targetScreen 버튼이 이동할 관리자 화면
     */
    public record RecommendedAction(ActionCode code, String displayText, TargetScreen targetScreen) { }

    /** HTML의 대표 조치 유형을 안정적인 서버 권장 행동 코드로 표현합니다. */
    public enum ActionCode {
        CAMPAIGN_NOT_READY,
        QUEUE_STALLED,
        ISSUANCE_STOPPED,
        CONSISTENCY_FAILURE,
        STOCK_DEPLETING,
        DATA_UNAVAILABLE
    }

    /** O1에서 서버가 판정해 화면에 표시하는 캠페인 발급 흐름 상태입니다. */
    public enum IssuanceFlowState {
        STOPPED,
        DECREASING,
        NORMAL
    }

    /** O2 대기 인원의 최근 변화 방향입니다. */
    public enum TrendDirection {
        INCREASING,
        DECREASING,
        UNCHANGED
    }

    /** O2에서 대기 깊이와 입장 처리율을 함께 보고 내린 운영 판정입니다. */
    public enum CampaignQueueAssessment {
        ADMISSION_STOPPED,
        GUIDANCE_THRESHOLD_EXCEEDED,
        NORMAL
    }

    /** O3에서 정책 결과와 시스템 문제를 섞지 않고 분리하는 고객 결과 코드입니다. */
    public enum CustomerOutcomeType {
        ISSUED,
        QUEUED,
        ALREADY_ISSUED,
        STOCK_EXHAUSTED,
        INELIGIBLE,
        ENTRY_EXPIRED,
        SYSTEM_FAILURE
    }

    /** 캠페인별 고객 영향의 범위를 제한된 서버 코드로 표현합니다. */
    public enum CustomerImpact {
        NONE,
        LIMITED,
        WIDESPREAD
    }

    /** 권장 행동 버튼이 이동할 관리자 화면을 제한된 코드로 표현합니다. */
    public enum TargetScreen {
        OVERVIEW,
        CAMPAIGN_DETAIL,
        METRICS,
        ISSUANCE_INQUIRY,
        NOTIFICATION_FAILURES
    }
}

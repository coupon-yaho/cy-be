package com.kafkick.api.admin.dashboard.dto;

import java.time.Instant;
import java.util.List;

import com.kafkick.api.admin.support.ObservedValue;
import com.kafkick.core.admin.Severity;
import com.kafkick.core.admin.SourceStatus;

/**
 * 관리자 첫 화면에 표시할 운영 위험과 조치 항목을 한 시점 기준으로 조립한 응답 초안입니다.
 *
 * <p>{@code overallStatus}는 전체 응답의 완전성({@code COMPLETE/PARTIAL/UNAVAILABLE})을 나타내고,
 * 각 {@link ObservedValue}의 상태는 개별 원천의 수집 상태를 나타냅니다. 수집하지 못한 값을 정상 또는 0으로
 * 위조하지 않으며 value를 null로 유지합니다. 완전성·심각도·조치 유형·고객 영향·대상 화면은
 * 명시적 enum으로 고정해 오타나 임의 코드를 허용하지 않습니다.</p>
 *
 * @param snapshotAt 이 응답이 나타내는 기준 시각
 * @param overallStatus 전체 응답 데이터의 완전성 상태
 * @param campaignRisk 캠페인 상태 기반 위험 요약과 해당 원천 상태
 * @param queueRisk 대기열 기준 초과 요약과 해당 원천 상태
 * @param stockRisk 재고·소진 위험 요약과 해당 원천 상태
 * @param actionItems 조치 항목 요약과 해당 원천 상태
 */
public record AdminOverviewResponse(
        Instant snapshotAt, OverallStatus overallStatus, ObservedValue<CampaignRiskSummary> campaignRisk,
        ObservedValue<QueueRiskSummary> queueRisk, ObservedValue<StockRiskSummary> stockRisk,
        ObservedValue<ActionItemSummary> actionItems) {
    /**
     * 모든 원천이 미수집 상태인 운영 개요 예시를 만듭니다.
     *
     * @param snapshotAt 예시에 사용할 기준 시각
     * @return 모든 개별 관측값이 UNAVAILABLE/null인 운영 개요
     */
    public static AdminOverviewResponse draft(Instant snapshotAt) {
        // 직렬화 계약 예시이므로 수집되지 않은 위험 수치를 0으로 채우지 않습니다.
        return new AdminOverviewResponse(
                snapshotAt,
                OverallStatus.UNAVAILABLE,
                new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null),
                new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null),
                new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null),
                new ObservedValue<>(null, SourceStatus.UNAVAILABLE, null)
        );
    }

    /**
     * 긴급·경고·오픈 임박·준비 미완료 캠페인 수를 구분한 요약입니다.
     *
     * @param urgentCampaignCount 즉시 조치가 필요한 캠페인 수
     * @param warningCampaignCount 경고 수준 캠페인 수
     * @param openingSoonCount 곧 오픈하지만 준비 확인이 필요한 캠페인 수
     * @param preparationIncompleteCount 필수 준비가 끝나지 않은 캠페인 수
     */
    public record CampaignRiskSummary(long urgentCampaignCount, long warningCampaignCount, long openingSoonCount, long preparationIncompleteCount) { }

    /**
     * B 대기열 Provider가 기준 초과로 판정한 캠페인 수입니다.
     *
     * @param thresholdExceededCount 대기 인원·시간 기준을 초과한 캠페인 수
     */
    public record QueueRiskSummary(long thresholdExceededCount) { }

    /**
     * DB 재고와 소진 예상 규칙에서 위험으로 판정한 캠페인 수입니다.
     *
     * @param depletionRiskCount 재고 부족 또는 소진 임박 캠페인 수
     */
    public record StockRiskSummary(long depletionRiskCount) { }

    /**
     * 전체 조치 건수와 화면에 우선 노출할 상위 20개 항목을 분리합니다.
     *
     * @param totalCount 전체 조치 필요 항목 수
     * @param topItems 심각도·감지 시각 기준으로 우선 노출할 상위 항목
     */
    public record ActionItemSummary(long totalCount, List<OperationActionItem> topItems) { }

    /**
     * 조치가 필요한 캠페인, 위험 수준, 감지 시각과 이동 대상 화면을 나타냅니다.
     *
     * @param couponId 조치 대상 쿠폰 캠페인 회차 식별자
     * @param severity 위험 심각도
     * @param type 조치 유형
     * @param customerImpact 고객 영향 범위
     * @param detectedAt 위험을 최초 감지한 시각
     * @param durationMillis 위험 지속 시간(ms); 계산할 수 없으면 null
     * @param targetScreen 운영자가 이동할 화면 코드
     */
    public record OperationActionItem(Long couponId, Severity severity, ActionItemType type,
                                      CustomerImpact customerImpact, Instant detectedAt,
                                      Long durationMillis, TargetScreen targetScreen) { }

    /** 전체 응답에 포함된 원천 데이터의 완전성입니다. */
    public enum OverallStatus { COMPLETE, PARTIAL, UNAVAILABLE }

    /** 운영자가 대응해야 하는 대표 위험 유형입니다. */
    public enum ActionItemType { CAMPAIGN_NOT_READY, QUEUE_STALLED, STOCK_DEPLETING, DATA_UNAVAILABLE }

    /** 위험이 고객에게 미치는 영향 범위입니다. */
    public enum CustomerImpact { NONE, LIMITED, WIDESPREAD }

    /** 조치 항목을 선택했을 때 이동할 관리자 화면입니다. */
    public enum TargetScreen { OVERVIEW, CAMPAIGN_DETAIL, METRICS, ISSUANCE_INQUIRY, NOTIFICATION_FAILURES }
}

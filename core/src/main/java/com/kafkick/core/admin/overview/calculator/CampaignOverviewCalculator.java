package com.kafkick.core.admin.overview.calculator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/**
 * 캠페인 원천 목록에서 관리자 운영현황의 캠페인 영역을 계산합니다.
 *
 * <p>Repository나 관측 저장소를 직접 조회하지 않고 전달받은 값만 사용하는 순수 계산 경계입니다.
 * 캠페인 상태 집계와 오픈 임박 판정을 담당하고, O1·O2·O4와 조치 대표 판정은 완료된 계산 결과를
 * couponId별로 조립합니다. 따라서 행·상단 KPI·조치 목록은 같은 계산 모집단을 재사용합니다.</p>
 */
@Component
public class CampaignOverviewCalculator {

    private static final Duration OPENING_SOON_WINDOW = Duration.ofMinutes(30);

    /** 상태가 없는 순수 계산기로 생성합니다. */
    public CampaignOverviewCalculator() { }

    /**
     * 동일한 기준 시각으로 캠페인 상태·오픈 임박과 O1·O2·O4·대표 조치를 한 행으로 조립합니다.
     *
     * @param snapshotAt 모든 시간 경계 판정에 사용하는 스냅샷 기준 시각
     * @param campaigns 계산할 캠페인 기본 원천 목록
     * @param issuanceFlows couponId별 O1 계산 완료 관측값
     * @param queueStatuses couponId별 O2 계산 완료 관측값
     * @param stockForecasts couponId별 O4 계산 완료 관측값
     * @param representativeActions Action 계산기가 전체 모집단에서 선택한 couponId별 대표 조치
     * @return 관리자 운영현황 조립에 사용할 캠페인 계산 결과
     * @throws NullPointerException 기준 시각, 목록 또는 목록 원소가 {@code null}인 경우
     */
    public CampaignCalculation calculate(
            Instant snapshotAt,
            List<CampaignOverviewSource> campaigns,
            java.util.Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows,
            java.util.Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus>> queueStatuses,
            java.util.Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast>> stockForecasts,
            java.util.Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeActions
    ) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        Objects.requireNonNull(campaigns, "campaigns");
        Objects.requireNonNull(issuanceFlows, "issuanceFlows");
        Objects.requireNonNull(queueStatuses, "queueStatuses");
        Objects.requireNonNull(stockForecasts, "stockForecasts");
        Objects.requireNonNull(representativeActions, "representativeActions");

        long openCount = 0L;
        long scheduledCount = 0L;
        long closedCount = 0L;
        long openingSoonCount = 0L;
        long preparationIncompleteCount = 0L;

        for (int index = 0; index < campaigns.size(); index++) {
            CampaignOverviewSource campaign = Objects.requireNonNull(
                    campaigns.get(index), "campaigns에는 null을 포함할 수 없습니다.");
            CouponStatus status = Objects.requireNonNull(campaign.status(), "campaign.status");

            switch (status) {
                case OPEN -> openCount++;
                case SCHEDULED -> scheduledCount++;
                case CLOSED -> closedCount++;
            }

            boolean openingSoon = isOpeningSoon(campaign, snapshotAt);
            if (openingSoon) {
                openingSoonCount++;
                if (!campaign.preparationCompleted()) {
                    preparationIncompleteCount++;
                }
            }
        }

        // 조치 심각도와 운영 상태를 먼저 반영한 뒤 행을 만들어 목록과 priority를 일치시킵니다.
        List<CampaignOverviewSource> prioritizedSources = campaigns.stream()
                .sorted(campaignPriority(snapshotAt, representativeActions))
                .toList();
        List<AdminOverviewSnapshot.CampaignOverview> calculatedCampaigns = new ArrayList<>();
        for (int index = 0; index < prioritizedSources.size(); index++) {
            CampaignOverviewSource campaign = prioritizedSources.get(index);
            boolean openingSoon = isOpeningSoon(campaign, snapshotAt);
            // 위험도 정렬이 끝난 목록에 순번을 부여해 입력 조회 순서가 priority에 새지 않게 합니다.
            calculatedCampaigns.add(toCampaignOverview(index + 1, campaign, issuanceFlows, queueStatuses,
                    stockForecasts, representativeActions));
        }

        return new CampaignCalculation(
                new AdminOverviewSnapshot.OpeningSoonSummary(
                        openingSoonCount, preparationIncompleteCount),
                new AdminOverviewSnapshot.CampaignStatusSummary(
                        openCount, scheduledCount, closedCount),
                calculatedCampaigns
        );
    }

    /**
     * O1·O2·O4 조립 입력을 아직 제공하지 않는 기존 호출을 모두 미연결 상태로 조립합니다.
     *
     * <p>이 호환 경로는 재고를 자체 계산하지 않습니다. 따라서 호출자는 값이 필요하면 Map을 받는
     * {@link #calculate(Instant, List, java.util.Map, java.util.Map, java.util.Map, java.util.Map)}를
     * 사용해야 하며, 누락 영역은 명시적으로 UNAVAILABLE입니다.</p>
     */
    public CampaignCalculation calculate(Instant snapshotAt, List<CampaignOverviewSource> campaigns) {
        return calculate(snapshotAt, campaigns, java.util.Map.of(), java.util.Map.of(), java.util.Map.of(),
                java.util.Map.of());
    }

    /** 정확히 30분 뒤에 오픈하는 예약 캠페인까지 운영자의 사전 확인 대상으로 포함합니다. */
    private static boolean isOpeningSoon(
            CampaignOverviewSource campaign,
            Instant snapshotAt
    ) {
        Instant opensAt = campaign.opensAt();
        return campaign.status() == CouponStatus.SCHEDULED
                && opensAt != null
                && opensAt.isAfter(snapshotAt)
                && !opensAt.isAfter(snapshotAt.plus(OPENING_SOON_WINDOW));
    }

    /** 위험 캠페인을 먼저 두고 동일 위험도에서는 운영상태·오픈 시각·ID로 순서를 고정합니다. */
    private static Comparator<CampaignOverviewSource> campaignPriority(
            Instant snapshotAt,
            java.util.Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeActions
    ) {
        return Comparator.comparing(
                        (CampaignOverviewSource campaign) -> severityOf(
                                campaign, snapshotAt, representativeActions),
                        Comparator.reverseOrder())
                .thenComparingInt(campaign -> statusPriority(campaign.status()))
                .thenComparing(
                        CampaignOverviewSource::opensAt,
                        Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparing(
                        CampaignOverviewSource::couponId,
                        Comparator.nullsLast(Comparator.naturalOrder()));
    }

    /** 전체 대표 Map의 심각도로 행 우선순위를 정해 상단 목록 절단과 무관한 순서를 보장합니다. */
    private static Severity severityOf(
            CampaignOverviewSource campaign,
            Instant snapshotAt,
            java.util.Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeActions
    ) {
        AdminOverviewSnapshot.OperationActionItem action = representativeActions.get(campaign.couponId());
        return action == null ? Severity.NONE : action.severity();
    }

    /** 같은 위험도에서는 운영 중, 오픈 예정, 종료 캠페인 순으로 확인하도록 상태 순위를 반환합니다. */
    private static int statusPriority(CouponStatus status) {
        return switch (status) {
            case OPEN -> 0;
            case SCHEDULED -> 1;
            case CLOSED -> 2;
        };
    }

    /**
     * 캠페인 기본 정보와 O1·O2·O4 관측 및 전체 모집단에서 확정된 대표 조치를 한 행으로 조립합니다.
     *
     * <p>Map에 없는 영역만 원천 미연결 {@code UNAVAILABLE}로 두며, 입력 Map이 명시한 {@code N_A},
     * {@code STALE} 등 계산기의 상태는 그대로 보존합니다. 행의 심각도·고객 영향·다음 행동은 화면의
     * 상위 20개 목록이 아니라 Action 계산기의 전체 대표 Map에서만 가져옵니다.</p>
     */
    private static AdminOverviewSnapshot.CampaignOverview toCampaignOverview(
            int priority,
            CampaignOverviewSource campaign,
            java.util.Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow>> issuanceFlows,
            java.util.Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus>> queueStatuses,
            java.util.Map<Long, AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast>> stockForecasts,
            java.util.Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeActions
    ) {
        AdminOverviewSnapshot.OperationActionItem representativeAction =
                representativeActions.get(campaign.couponId());
        Severity severity = representativeAction == null ? Severity.NONE : representativeAction.severity();
        AdminOverviewSnapshot.CustomerImpact customerImpact = representativeAction == null
                ? AdminOverviewSnapshot.CustomerImpact.NONE : representativeAction.customerImpact();
        String customerImpactText = representativeAction == null
                ? null : representativeAction.customerImpactText();
        AdminOverviewSnapshot.RecommendedAction recommendedAction = representativeAction == null
                ? null : representativeAction.recommendedAction();

        return new AdminOverviewSnapshot.CampaignOverview(
                priority,
                campaign.couponId(),
                campaign.campaignName(),
                campaign.brandName(),
                campaign.status(),
                campaign.opensAt(),
                campaign.closesAt(),
                severity,
                observationOrUnavailable(issuanceFlows, campaign.couponId()),
                observationOrUnavailable(queueStatuses, campaign.couponId()),
                observationOrUnavailable(stockForecasts, campaign.couponId()),
                customerImpact,
                customerImpactText,
                recommendedAction
        );
    }

    /** Map에 값이 없을 때만 원천 미연결을 표시하고 계산 결과의 명시적 상태는 덮어쓰지 않습니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> observationOrUnavailable(
            java.util.Map<Long, AdminOverviewSnapshot.Observation<T>> observations,
            Long couponId
    ) {
        AdminOverviewSnapshot.Observation<T> observation = observations.get(couponId);
        return observation == null ? unavailableObservation() : observation;
    }

    /** 실제로 수집하지 않은 독립 원천을 공통 Core 상태 규칙에 맞춰 생성합니다. */
    private static <T> AdminOverviewSnapshot.Observation<T> unavailableObservation() {
        return new AdminOverviewSnapshot.Observation<>(null, SourceStatus.UNAVAILABLE, null);
    }

    /**
     * 캠페인 원천 목록에서 함께 계산한 상단 KPI와 캠페인별 표시 결과입니다.
     *
     * @param openingSoon 30분 안에 오픈하는 캠페인과 그중 준비 미완료 캠페인 수
     * @param campaignStatusSummary 캠페인의 진행·예정·종료 상태별 수
     * @param campaigns 캠페인 기본 정보와 독립적인 O1·O2·O4 원천 상태 목록
     */
    public record CampaignCalculation(
            AdminOverviewSnapshot.OpeningSoonSummary openingSoon,
            AdminOverviewSnapshot.CampaignStatusSummary campaignStatusSummary,
            List<AdminOverviewSnapshot.CampaignOverview> campaigns
    ) {

        /** 호출 이후 원천 목록 변경이 계산 결과에 영향을 주지 않도록 불변 복사합니다. */
        public CampaignCalculation {
            Objects.requireNonNull(openingSoon, "openingSoon");
            Objects.requireNonNull(campaignStatusSummary, "campaignStatusSummary");
            Objects.requireNonNull(campaigns, "campaigns");
            campaigns = List.copyOf(campaigns);
        }
    }
}

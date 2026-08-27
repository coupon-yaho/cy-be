package com.kafkick.core.admin.overview.calculator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.core.admin.campaignsource.PreparationObservation;
import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/**
 * 캠페인 원천 목록에서 관리자 운영현황의 캠페인 영역을 계산합니다.
 *
 * <p>Repository나 관측 저장소를 직접 조회하지 않고 전달받은 값만 사용하는 순수 계산 경계입니다.
 * 캠페인 상태 집계·우선순위와 O1·O2·O4 및 조치 대표 판정을 couponId별로 조립합니다. 준비 상태는
 * 별도 계산 경계에서 오픈 임박 KPI와 조치 후보를 함께 만듭니다.</p>
 */
@Component
public class CampaignOverviewCalculator {

    private static final Duration OPENING_SOON_WINDOW = Duration.ofMinutes(30);

    /** 상태가 없는 순수 계산기로 생성합니다. */
    public CampaignOverviewCalculator() { }

    /**
     * 동일한 기준 시각으로 캠페인 상태와 O1·O2·O4·대표 조치를 한 행으로 조립합니다.
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
        for (int index = 0; index < campaigns.size(); index++) {
            CampaignOverviewSource campaign = Objects.requireNonNull(
                    campaigns.get(index), "campaigns에는 null을 포함할 수 없습니다.");
            CouponRoundStatus status = Objects.requireNonNull(campaign.status(), "campaign.status");

            switch (status) {
                case OPEN -> openCount++;
                case SCHEDULED -> scheduledCount++;
                case CLOSED -> closedCount++;
            }
        }

        // 조치 심각도와 운영 상태를 먼저 반영한 뒤 행을 만들어 목록과 priority를 일치시킵니다.
        List<CampaignOverviewSource> prioritizedSources = campaigns.stream()
                .sorted(campaignPriority(snapshotAt, representativeActions))
                .toList();
        List<AdminOverviewSnapshot.CampaignOverview> calculatedCampaigns = new ArrayList<>();
        for (int index = 0; index < prioritizedSources.size(); index++) {
            CampaignOverviewSource campaign = prioritizedSources.get(index);
            // 위험도 정렬이 끝난 목록에 순번을 부여해 입력 조회 순서가 priority에 새지 않게 합니다.
            calculatedCampaigns.add(toCampaignOverview(index + 1, campaign, issuanceFlows, queueStatuses,
                    stockForecasts, representativeActions));
        }

        return new CampaignCalculation(
                new AdminOverviewSnapshot.CampaignStatusSummary(
                        openCount, scheduledCount, closedCount),
                calculatedCampaigns
        );
    }

    /**
     * 오픈 임박 예약 캠페인의 준비 KPI와 확인 조치 후보를 같은 모집단에서 한 번에 계산합니다.
     *
     * <p>준비 완료가 확정된 {@code VALID true}는 KPI에만 포함하고, {@code VALID false}는 오픈 30분 전을
     * 감지 시각으로 한 확인 조치를 만듭니다. {@code PENDING}, {@code UNAVAILABLE}과 같은 값 없는 상태는
     * false로 바꾸지 않고 KPI 상태에 보존합니다.</p>
     *
     * @param snapshotAt 모든 시간 경계 판정에 사용하는 스냅샷 기준 시각
     * @param campaigns 계산할 캠페인 기본 원천 목록
     * @return 오픈 임박 KPI와 준비 미완료 조치 후보
     * @throws NullPointerException 기준 시각, 목록 또는 목록 원소가 {@code null}인 경우
     */
    public PreparationCalculation calculatePreparation(
            Instant snapshotAt,
            List<CampaignOverviewSource> campaigns
    ) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        Objects.requireNonNull(campaigns, "campaigns");

        long openingSoonCount = 0L;
        long preparationIncompleteCount = 0L;
        SourceStatus preparationStatus = SourceStatus.VALID;
        Instant preparationObservedAt = null;
        List<AdminOverviewSnapshot.OperationActionItem> actionCandidates = new ArrayList<>();

        for (int index = 0; index < campaigns.size(); index++) {
            CampaignOverviewSource campaign = Objects.requireNonNull(
                    campaigns.get(index), "campaigns에는 null을 포함할 수 없습니다.");
            // 예약 상태이며 스냅샷부터 30분 뒤까지 오픈하는 캠페인만 준비 판단 모집단으로 둡니다.
            if (!isOpeningSoon(campaign, snapshotAt)) {
                continue;
            }

            openingSoonCount++;
            PreparationObservation preparation = campaign.preparation();
            // 값 없는 준비 상태를 false로 보정하지 않고 오픈 임박 KPI의 상태로 합성합니다.
            preparationStatus = combinePreparationStatus(preparationStatus, preparation.status());
            if (preparation.status().carriesValue()) {
                // 값이 있는 준비 관측 중 가장 오래된 실제 시각을 집계 KPI의 기준 시각으로 보존합니다.
                preparationObservedAt = earlier(preparationObservedAt, preparation.observedAt());
            }
            if (!Boolean.FALSE.equals(preparation.completed())) {
                continue;
            }

            preparationIncompleteCount++;
            // 확정 또는 마지막 값이 false인 준비 관측만 오픈 30분 전 확인 조치 후보로 만듭니다.
            if (preparation.status() == SourceStatus.VALID || preparation.status() == SourceStatus.STALE) {
                actionCandidates.add(preparationActionCandidate(campaign));
            }
        }

        Instant observedAt = openingSoonCount == 0L ? snapshotAt : preparationObservedAt;
        return new PreparationCalculation(openingSoonObservation(
                openingSoonCount, preparationIncompleteCount, preparationStatus, observedAt), actionCandidates);
    }

    /**
     * 오픈 임박 캠페인의 준비 관측 상태를 상단 KPI에 보존합니다.
     *
     * <p>PENDING 준비 상태는 미완료 0건으로 보정하지 않습니다. 값 없는 준비 상태가 하나라도 있으면
     * 상단 KPI 전체를 해당 값 없는 상태로 둡니다.</p>
     */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.OpeningSoonSummary>
            openingSoonObservation(
                    long openingSoonCount,
                    long preparationIncompleteCount,
                    SourceStatus preparationStatus,
                    Instant observedAt
            ) {
        if (!preparationStatus.carriesValue()) {
            return new AdminOverviewSnapshot.Observation<>(null, preparationStatus, null);
        }
        return new AdminOverviewSnapshot.Observation<>(
                new AdminOverviewSnapshot.OpeningSoonSummary(openingSoonCount, preparationIncompleteCount),
                preparationStatus,
                observedAt);
    }

    /** 여러 준비 값으로 만든 집계가 실제보다 새로 보이지 않도록 가장 오래된 관측 시각을 선택합니다. */
    private static Instant earlier(Instant left, Instant right) {
        return left == null || right.isBefore(left) ? right : left;
    }

    /** 값 없는 준비 상태가 알려진 수치를 정상값처럼 덮어쓰지 않도록 우선순위를 합성합니다. */
    private static SourceStatus combinePreparationStatus(SourceStatus current, SourceStatus next) {
        if (current == SourceStatus.UNAVAILABLE || next == SourceStatus.UNAVAILABLE) {
            return SourceStatus.UNAVAILABLE;
        }
        if (current == SourceStatus.PENDING || next == SourceStatus.PENDING) {
            return SourceStatus.PENDING;
        }
        if (current == SourceStatus.N_A || next == SourceStatus.N_A) {
            return SourceStatus.N_A;
        }
        if (current == SourceStatus.STALE || next == SourceStatus.STALE) {
            return SourceStatus.STALE;
        }
        if (current == SourceStatus.WARMING_UP || next == SourceStatus.WARMING_UP) {
            return SourceStatus.WARMING_UP;
        }
        if (current == SourceStatus.NO_TRAFFIC || next == SourceStatus.NO_TRAFFIC) {
            return SourceStatus.NO_TRAFFIC;
        }
        return SourceStatus.VALID;
    }

    /** 스냅샷 시각부터 정확히 30분 뒤까지 오픈하는 예약 캠페인을 운영자의 사전 확인 대상으로 포함합니다. */
    private static boolean isOpeningSoon(
            CampaignOverviewSource campaign,
            Instant snapshotAt
    ) {
        Instant opensAt = campaign.opensAt();
        return campaign.status() == CouponRoundStatus.SCHEDULED
                && opensAt != null
                && !opensAt.isBefore(snapshotAt)
                && !opensAt.isAfter(snapshotAt.plus(OPENING_SOON_WINDOW));
    }

    /** 준비가 확인되지 않은 오픈 임박 캠페인을 위한 서버 제공 조치 후보를 만듭니다. */
    private static AdminOverviewSnapshot.OperationActionItem preparationActionCandidate(
            CampaignOverviewSource campaign
    ) {
        return new AdminOverviewSnapshot.OperationActionItem(
                campaign.couponId(),
                campaign.campaignName(),
                campaign.opensAt(),
                Severity.WARN,
                AdminOverviewSnapshot.CustomerImpact.NONE,
                "오픈 전 필수 준비 항목을 확인해야 합니다.",
                campaign.opensAt().minus(OPENING_SOON_WINDOW),
                null,
                new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY,
                        "캠페인 준비 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL));
    }

    /** 위험 캠페인을 먼저 두고 동일 위험도에서는 운영상태·오픈 시각·ID로 순서를 고정합니다. */
    private static Comparator<CampaignOverviewSource> campaignPriority(
            Instant snapshotAt,
            java.util.Map<Long, AdminOverviewSnapshot.OperationActionItem> representativeActions
    ) {
        return Comparator.comparingInt(
                        (CampaignOverviewSource campaign) -> severityRank(
                                severityOf(campaign, snapshotAt, representativeActions)))
                .reversed()
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

    /** enum 선언 순서와 무관하게 캠페인 위험 노출 순위를 고정합니다. */
    private static int severityRank(Severity severity) {
        return switch (severity) {
            case NONE -> 0;
            case WARN -> 1;
            case CRITICAL -> 2;
        };
    }

    /** 같은 위험도에서는 운영 중, 오픈 예정, 종료 캠페인 순으로 확인하도록 상태 순위를 반환합니다. */
    private static int statusPriority(CouponRoundStatus status) {
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
                campaign.preparation().failedItems(),
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
     * 캠페인 원천 목록에서 계산한 상태 집계와 캠페인별 표시 결과입니다.
     *
     * @param campaignStatusSummary 캠페인의 진행·예정·종료 상태별 수
     * @param campaigns 캠페인 기본 정보와 독립적인 O1·O2·O4 원천 상태 목록
     */
    public record CampaignCalculation(
            AdminOverviewSnapshot.CampaignStatusSummary campaignStatusSummary,
            List<AdminOverviewSnapshot.CampaignOverview> campaigns
    ) {

        /** 호출 이후 원천 목록 변경이 계산 결과에 영향을 주지 않도록 불변 복사합니다. */
        public CampaignCalculation {
            Objects.requireNonNull(campaignStatusSummary, "campaignStatusSummary");
            Objects.requireNonNull(campaigns, "campaigns");
            campaigns = List.copyOf(campaigns);
        }
    }

    /**
     * 오픈 임박 준비 상태의 상단 KPI와 조치 계산기가 확정할 후보 목록입니다.
     *
     * @param openingSoon 30분 안에 오픈하는 캠페인과 준비 관측 상태
     * @param actionCandidates 준비 미완료가 확정된 캠페인의 조치 후보
     */
    public record PreparationCalculation(
            AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.OpeningSoonSummary> openingSoon,
            List<AdminOverviewSnapshot.OperationActionItem> actionCandidates
    ) {

        /** KPI와 조치 후보를 값 없는 상태나 외부 목록 변경 없이 함께 보존합니다. */
        public PreparationCalculation {
            Objects.requireNonNull(openingSoon, "openingSoon");
            Objects.requireNonNull(actionCandidates, "actionCandidates");
            actionCandidates = List.copyOf(actionCandidates);
        }
    }
}

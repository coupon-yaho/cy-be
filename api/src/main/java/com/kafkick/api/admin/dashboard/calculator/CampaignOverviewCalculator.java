package com.kafkick.api.admin.dashboard.calculator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.api.admin.dashboard.model.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;
import com.kafkick.core.observation.SourceStatus;

/**
 * 캠페인 원천 목록에서 관리자 운영현황의 캠페인 영역을 계산합니다.
 *
 * <p>Repository나 관측 저장소를 직접 조회하지 않고 전달받은 값만 사용하는 순수 계산 경계입니다.
 * 캠페인 상태 집계, 오픈 임박 판정과 V1 재고 계산을 담당하며, 발급·대기열처럼 아직 원천이 없는
 * 값은 명시적인 {@link SourceStatus#UNAVAILABLE}로 보존합니다.</p>
 */
@Component
public class CampaignOverviewCalculator {

    private static final Duration OPENING_SOON_WINDOW = Duration.ofMinutes(30);

    /** 상태가 없는 순수 계산기로 생성합니다. */
    public CampaignOverviewCalculator() { }

    /**
     * 동일한 기준 시각으로 캠페인 상태, 오픈 임박 여부와 표시용 재고를 계산합니다.
     *
     * @param snapshotAt 모든 시간 경계 판정에 사용하는 스냅샷 기준 시각
     * @param campaigns 계산할 캠페인 원천 목록
     * @return 관리자 운영현황 조립에 사용할 캠페인 계산 결과
     * @throws NullPointerException 기준 시각, 목록 또는 목록 원소가 {@code null}인 경우
     */
    public CampaignCalculation calculate(
            Instant snapshotAt,
            List<CampaignOverviewSource> campaigns
    ) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        Objects.requireNonNull(campaigns, "campaigns");

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

        List<CampaignOverviewSource> prioritizedSources = campaigns.stream()
                .sorted(campaignPriority(snapshotAt))
                .toList();
        List<AdminOverviewSnapshot.CampaignOverview> calculatedCampaigns = new ArrayList<>();
        for (int index = 0; index < prioritizedSources.size(); index++) {
            CampaignOverviewSource campaign = prioritizedSources.get(index);
            boolean openingSoon = isOpeningSoon(campaign, snapshotAt);
            // 위험도 정렬이 끝난 목록에 순번을 부여해 입력 조회 순서가 priority에 새지 않게 합니다.
            calculatedCampaigns.add(toCampaignOverview(index + 1, campaign, openingSoon));
        }

        return new CampaignCalculation(
                new AdminOverviewSnapshot.OpeningSoonSummary(
                        openingSoonCount, preparationIncompleteCount),
                new AdminOverviewSnapshot.CampaignStatusSummary(
                        openCount, scheduledCount, closedCount),
                calculatedCampaigns
        );
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
    private static Comparator<CampaignOverviewSource> campaignPriority(Instant snapshotAt) {
        return Comparator.comparing(
                        (CampaignOverviewSource campaign) -> severityOf(campaign, snapshotAt),
                        Comparator.reverseOrder())
                .thenComparingInt(campaign -> statusPriority(campaign.status()))
                .thenComparing(
                        CampaignOverviewSource::opensAt,
                        Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparing(
                        CampaignOverviewSource::couponId,
                        Comparator.nullsLast(Comparator.naturalOrder()));
    }

    /** 현재 계산 가능한 준비 미완료 위험을 캠페인 행의 심각도로 변환합니다. */
    private static Severity severityOf(
            CampaignOverviewSource campaign,
            Instant snapshotAt
    ) {
        return isOpeningSoon(campaign, snapshotAt) && !campaign.preparationCompleted()
                ? Severity.WARN
                : Severity.NONE;
    }

    /** 같은 위험도에서는 운영 중, 오픈 예정, 종료 캠페인 순으로 확인하도록 상태 순위를 반환합니다. */
    private static int statusPriority(CouponStatus status) {
        return switch (status) {
            case OPEN -> 0;
            case SCHEDULED -> 1;
            case CLOSED -> 2;
        };
    }

    /** 캠페인 기본 정보와 독립적인 O1·O2·O4 원천 상태를 한 행으로 조립합니다. */
    private static AdminOverviewSnapshot.CampaignOverview toCampaignOverview(
            int priority,
            CampaignOverviewSource campaign,
            boolean openingSoon
    ) {
        boolean preparationIncomplete = openingSoon && !campaign.preparationCompleted();
        Severity severity = preparationIncomplete ? Severity.WARN : Severity.NONE;
        String customerImpactText = preparationIncomplete
                ? "오픈 전 필수 준비 항목을 확인해야 합니다."
                : null;
        AdminOverviewSnapshot.RecommendedAction recommendedAction = preparationIncomplete
                ? new AdminOverviewSnapshot.RecommendedAction(
                        AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY,
                        "캠페인 준비 상태 확인",
                        AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL)
                : null;

        // 발급과 대기열 원천은 Mock 값으로 추정하지 않고 실제 관측 연결 전까지 수집 불가로 둡니다.
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.IssuanceFlow> issuanceFlow =
                unavailableObservation();
        AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.CampaignQueueStatus> queueStatus =
                unavailableObservation();

        return new AdminOverviewSnapshot.CampaignOverview(
                priority,
                campaign.couponId(),
                campaign.campaignName(),
                campaign.brandName(),
                campaign.status(),
                campaign.opensAt(),
                campaign.closesAt(),
                severity,
                issuanceFlow,
                queueStatus,
                calculateStock(campaign),
                AdminOverviewSnapshot.CustomerImpact.NONE,
                customerImpactText,
                recommendedAction
        );
    }

    /** V1에서 확인 가능한 DB 재고만 계산하고 Redis 기반 엔진은 실제 원천 연결을 기다립니다. */
    private static AdminOverviewSnapshot.Observation<AdminOverviewSnapshot.StockForecast>
    calculateStock(CampaignOverviewSource campaign) {
        if (!hasValidV1Stock(campaign)) {
            // 누락·역전된 수량이나 Redis 재고를 0으로 보정하면 정상 소진으로 오해할 수 있습니다.
            return unavailableObservation();
        }

        long totalQuantity = campaign.totalQuantity();
        long remainingQuantity = totalQuantity - campaign.activeCount();
        double remainingRatio = remainingQuantity / (double) totalQuantity;

        // 최근 성공 발급률이 없으므로 예상 소진시간은 임의 계산하지 않습니다.
        AdminOverviewSnapshot.StockForecast stockForecast =
                new AdminOverviewSnapshot.StockForecast(
                        remainingQuantity,
                        totalQuantity,
                        remainingRatio,
                        null
                );
        return new AdminOverviewSnapshot.Observation<>(
                stockForecast,
                SourceStatus.VALID,
                campaign.stockObservedAt()
        );
    }

    /** V1 재고 계산에 필요한 값이 전부 존재하고 수량 관계가 유효한지 확인합니다. */
    private static boolean hasValidV1Stock(CampaignOverviewSource campaign) {
        if (campaign.engineVersion() != EngineVersion.V1
                || campaign.totalQuantity() == null
                || campaign.activeCount() == null
                || campaign.stockObservedAt() == null) {
            return false;
        }
        return campaign.totalQuantity() > 0L
                && campaign.activeCount() >= 0L
                && campaign.activeCount() <= campaign.totalQuantity();
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

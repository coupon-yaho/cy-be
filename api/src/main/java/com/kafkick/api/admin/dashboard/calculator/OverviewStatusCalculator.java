package com.kafkick.api.admin.dashboard.calculator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.api.admin.dashboard.AdminOverviewResult.OverallStatus;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.observation.SourceStatus;

/**
 * 관리자 운영현황의 원천별 상태를 전체 응답 완전성으로 계산합니다.
 *
 * <p>캠페인·발급·대기열·재고를 핵심 원천 그룹으로 취급하며 HTTP 지연은 보조 원천으로
 * 취급합니다. 조치 KPI와 목록은 다른 원천에서 파생한 결과이므로 독립 원천으로 중복 계산하지
 * 않습니다. Repository나 관측 저장소를 조회하지 않고 완성된 Snapshot의 상태만 판정합니다.</p>
 */
@Component
public class OverviewStatusCalculator {

    /**
     * 최상위 및 캠페인 내부 관측 상태를 전체 응답 완전성으로 축약합니다.
     *
     * <p>{@link SourceStatus#N_A N_A}는 현재 조건에 적용되지 않는 상태이므로 완전성을 낮추지
     * 않습니다. 적용 가능한 모든 원천이 {@code VALID} 또는 {@code NO_TRAFFIC}이면 COMPLETE이며,
     * 일부 원천만 해석 가능하면 PARTIAL입니다. 해석 가능한 핵심 원천이 하나도 없으면 HTTP 지연이
     * 존재하더라도 UNAVAILABLE입니다.</p>
     *
     * @param snapshot 원천별 값과 상태가 조립된 운영현황 Snapshot
     * @return 전체 응답의 COMPLETE, PARTIAL 또는 UNAVAILABLE 상태
     * @throws NullPointerException snapshot이 null인 경우
     */
    public OverallStatus calculate(AdminOverviewSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        List<List<SourceStatus>> coreGroups = List.of(
                campaignSourceStatuses(snapshot),
                issuanceSourceStatuses(snapshot),
                queueSourceStatuses(snapshot),
                stockSourceStatuses(snapshot)
        );
        List<List<SourceStatus>> allGroups = new ArrayList<>(coreGroups);
        allGroups.add(List.of(statusOf(snapshot.latencySummary())));

        boolean hasUsableCoreSource = coreGroups.stream()
                .anyMatch(OverviewStatusCalculator::groupHasUsableValue);
        if (!hasUsableCoreSource) {
            return OverallStatus.UNAVAILABLE;
        }

        List<List<SourceStatus>> applicableGroups = allGroups.stream()
                .filter(OverviewStatusCalculator::isApplicableGroup)
                .toList();
        if (applicableGroups.stream().allMatch(OverviewStatusCalculator::isCompleteGroup)) {
            return OverallStatus.COMPLETE;
        }
        return OverallStatus.PARTIAL;
    }

    /** 캠페인·설정 원천이 만드는 오픈 임박, 상태 집계, 기본 목록 상태를 묶습니다. */
    private static List<SourceStatus> campaignSourceStatuses(AdminOverviewSnapshot snapshot) {
        return List.of(
                statusOf(snapshot.openingSoon()),
                statusOf(snapshot.campaignStatusSummary()),
                statusOf(snapshot.campaigns())
        );
    }

    /** 발급 관측 원천이 만드는 전체 발급률, 고객 결과, 캠페인별 O1 상태를 묶습니다. */
    private static List<SourceStatus> issuanceSourceStatuses(AdminOverviewSnapshot snapshot) {
        List<SourceStatus> statuses = new ArrayList<>();
        statuses.add(statusOf(snapshot.aggregateIssuanceRate()));
        statuses.add(statusOf(snapshot.customerOutcomes()));
        campaigns(snapshot).forEach(campaign -> statuses.add(statusOf(campaign.issuanceFlow())));
        return statuses;
    }

    /** 대기열 원천이 만드는 상단 위험, 전체 대기, 캠페인별 O2 상태를 묶습니다. */
    private static List<SourceStatus> queueSourceStatuses(AdminOverviewSnapshot snapshot) {
        List<SourceStatus> statuses = new ArrayList<>();
        statuses.add(statusOf(snapshot.queueRisk()));
        statuses.add(statusOf(snapshot.aggregateQueue()));
        campaigns(snapshot).forEach(
                campaign -> statuses.add(statusOf(campaign.campaignQueueStatus())));
        return statuses;
    }

    /** 재고 원천이 만드는 상단 소진 위험과 캠페인별 O4 상태를 묶습니다. */
    private static List<SourceStatus> stockSourceStatuses(AdminOverviewSnapshot snapshot) {
        List<SourceStatus> statuses = new ArrayList<>();
        statuses.add(statusOf(snapshot.stockRisk()));
        campaigns(snapshot).forEach(campaign -> statuses.add(statusOf(campaign.stockForecast())));
        return statuses;
    }

    /** 기본 목록을 읽지 못했으면 중첩 상태를 임의 생성하지 않고 빈 모집단을 반환합니다. */
    private static List<AdminOverviewSnapshot.CampaignOverview> campaigns(
            AdminOverviewSnapshot snapshot
    ) {
        if (snapshot.campaigns() == null || snapshot.campaigns().value() == null) {
            return List.of();
        }
        return snapshot.campaigns().value();
    }

    /** 누락된 관측 영역은 정상값이 아니라 명시적인 미수집 상태로 취급합니다. */
    private static SourceStatus statusOf(AdminOverviewSnapshot.Observation<?> observation) {
        return observation == null ? SourceStatus.UNAVAILABLE : observation.status();
    }

    /** 모든 상태가 N_A인 원천 그룹을 현재 실행 조건의 완전성 모집단에서 제외합니다. */
    private static boolean isApplicableGroup(List<SourceStatus> statuses) {
        return statuses.stream().anyMatch(status -> status != SourceStatus.N_A);
    }

    /** 원천 그룹에 화면이 해석할 수 있는 실제 값이 하나 이상 있는지 확인합니다. */
    private static boolean groupHasUsableValue(List<SourceStatus> statuses) {
        return statuses.stream()
                .filter(status -> status != SourceStatus.N_A)
                .anyMatch(OverviewStatusCalculator::hasUsableValue);
    }

    /** N_A를 제외한 원천 그룹의 모든 상태가 최종 관측 상태인지 확인합니다. */
    private static boolean isCompleteGroup(List<SourceStatus> statuses) {
        return statuses.stream()
                .filter(status -> status != SourceStatus.N_A)
                .allMatch(OverviewStatusCalculator::isComplete);
    }

    /** 전체 응답을 COMPLETE로 표시할 수 있는 최종 관측 상태인지 확인합니다. */
    private static boolean isComplete(SourceStatus status) {
        return status == SourceStatus.VALID || status == SourceStatus.NO_TRAFFIC;
    }

    /** 부분 응답이라도 화면이 해석할 실제 값이 존재하는 상태인지 확인합니다. */
    private static boolean hasUsableValue(SourceStatus status) {
        return isComplete(status)
                || status == SourceStatus.WARMING_UP
                || status == SourceStatus.STALE;
    }
}

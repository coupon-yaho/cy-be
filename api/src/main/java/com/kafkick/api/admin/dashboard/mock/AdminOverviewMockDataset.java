package com.kafkick.api.admin.dashboard.mock;

import java.util.List;
import java.util.Objects;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;

/**
 * 한 관리자 운영현황 스냅샷에 사용할 Mock 캠페인 원천과 조치 후보를 함께 보존합니다.
 *
 * <p>두 목록을 같은 Dataset으로 묶어 캠페인 기본 목록과 그 캠페인에서 파생된 조치 판정이 서로
 * 다른 기준 시각이나 모집단을 사용하지 않도록 합니다.</p>
 *
 * @param campaigns 캠페인 상태·오픈 임박·재고 계산에 사용할 원천 목록
 * @param actionCandidates 조치 집계 계산기에 전달할 판정 완료 후보 목록
 */
public record AdminOverviewMockDataset(
        List<CampaignOverviewSource> campaigns,
        List<AdminOverviewSnapshot.OperationActionItem> actionCandidates
) {

    /** 외부 변경으로 한 응답의 Mock 모집단이 달라지지 않도록 두 목록을 불변 복사합니다. */
    public AdminOverviewMockDataset {
        Objects.requireNonNull(campaigns, "campaigns");
        Objects.requireNonNull(actionCandidates, "actionCandidates");
        campaigns = List.copyOf(campaigns);
        actionCandidates = List.copyOf(actionCandidates);
    }
}

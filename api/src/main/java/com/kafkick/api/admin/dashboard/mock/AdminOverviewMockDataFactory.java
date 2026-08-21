package com.kafkick.api.admin.dashboard.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Component;

import com.kafkick.api.admin.dashboard.model.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.EngineVersion;
import com.kafkick.core.observation.Severity;

/**
 * 캠페인 저장소가 준비되기 전 관리자 운영현황 계산에 사용할 고정 시나리오를 생성합니다.
 *
 * <p>실행 날짜에 따라 오픈 임박 판정이 달라지지 않도록 절대 날짜를 저장하지 않고, 호출자가 전달한
 * 스냅샷 시각을 기준으로 모든 캠페인 시각을 상대적으로 생성합니다.</p>
 */
@Component
public class AdminOverviewMockDataFactory {

    /**
     * 운영 중·오픈 임박·준비 미완료·종료 상황을 포함한 Mock Dataset을 생성합니다.
     *
     * @param snapshotAt 캠페인 시각과 조치 감지 시각을 만드는 기준 시각
     * @return 동일한 기준 시각으로 생성한 캠페인 원천과 조치 후보
     * @throws NullPointerException snapshotAt이 {@code null}인 경우
     */
    public AdminOverviewMockDataset create(Instant snapshotAt) {
        Objects.requireNonNull(snapshotAt, "snapshotAt");

        CampaignOverviewSource openCampaign = new CampaignOverviewSource(
                101L,
                "여름 특가 쿠폰",
                "카프킥",
                CouponStatus.OPEN,
                snapshotAt.minus(Duration.ofHours(1)),
                snapshotAt.plus(Duration.ofHours(2)),
                EngineVersion.V1,
                1_000L,
                700L,
                snapshotAt,
                true
        );
        CampaignOverviewSource scheduledCampaign = new CampaignOverviewSource(
                102L,
                "가을 얼리버드 쿠폰",
                "카프킥",
                CouponStatus.SCHEDULED,
                snapshotAt.plus(Duration.ofMinutes(20)),
                snapshotAt.plus(Duration.ofHours(4)),
                EngineVersion.V1,
                500L,
                0L,
                snapshotAt,
                true
        );
        CampaignOverviewSource incompleteCampaign = new CampaignOverviewSource(
                103L,
                "주말 한정 쿠폰",
                "카프킥",
                CouponStatus.SCHEDULED,
                snapshotAt.plus(Duration.ofMinutes(10)),
                snapshotAt.plus(Duration.ofHours(3)),
                EngineVersion.V1,
                null,
                null,
                null,
                false
        );
        CampaignOverviewSource closedCampaign = new CampaignOverviewSource(
                104L,
                "종료된 시즌 쿠폰",
                "카프킥",
                CouponStatus.CLOSED,
                snapshotAt.minus(Duration.ofHours(5)),
                snapshotAt.minus(Duration.ofHours(1)),
                EngineVersion.V1,
                300L,
                300L,
                snapshotAt,
                true
        );

        // 준비 미완료 판정은 Mock 원천에서 확정하고 집계 계산기는 판정 결과만 소비합니다.
        AdminOverviewSnapshot.OperationActionItem incompleteAction =
                new AdminOverviewSnapshot.OperationActionItem(
                        incompleteCampaign.couponId(),
                        incompleteCampaign.campaignName(),
                        incompleteCampaign.opensAt(),
                        Severity.WARN,
                        AdminOverviewSnapshot.CustomerImpact.NONE,
                        "오픈 전 필수 준비 항목을 확인해야 합니다.",
                        snapshotAt,
                        null,
                        new AdminOverviewSnapshot.RecommendedAction(
                                AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY,
                                "캠페인 준비 상태 확인",
                                AdminOverviewSnapshot.TargetScreen.CAMPAIGN_DETAIL)
                );

        return new AdminOverviewMockDataset(
                List.of(openCampaign, scheduledCampaign, incompleteCampaign, closedCampaign),
                List.of(incompleteAction)
        );
    }
}

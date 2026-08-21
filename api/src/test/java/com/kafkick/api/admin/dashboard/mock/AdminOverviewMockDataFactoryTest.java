package com.kafkick.api.admin.dashboard.mock;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.kafkick.core.admin.overview.CampaignOverviewSource;
import com.kafkick.core.admin.overview.AdminOverviewSnapshot;
import com.kafkick.core.coupon.CouponStatus;

/** 관리자 운영현황 선행 구현에 사용할 캠페인 상황별 Mock 원천을 검증합니다. */
class AdminOverviewMockDataFactoryTest {

    private static final Instant SNAPSHOT_AT = Instant.parse("2026-08-21T03:00:00Z");

    /** 실행 날짜와 무관하게 동일한 상대 시각과 운영 상태를 제공하는지 검증합니다. */
    @Test
    @DisplayName("Mock 캠페인은 운영 중·오픈 임박·준비 미완료·종료 상황을 제공한다")
    void createsOperationalCampaignScenariosRelativeToSnapshotTime() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);

        assertThat(dataset.campaigns())
                .extracting(CampaignOverviewSource::status)
                .containsExactly(
                        CouponStatus.OPEN,
                        CouponStatus.SCHEDULED,
                        CouponStatus.SCHEDULED,
                        CouponStatus.CLOSED);
        assertThat(dataset.campaigns().get(1).opensAt())
                .isEqualTo(SNAPSHOT_AT.plus(Duration.ofMinutes(20)));
        assertThat(dataset.campaigns().get(2).opensAt())
                .isEqualTo(SNAPSHOT_AT.plus(Duration.ofMinutes(10)));
        assertThat(dataset.campaigns().get(2).preparationCompleted()).isFalse();
    }

    /** 준비 미완료 상황이 화면 조치 KPI와 목록에 사용할 판정 후보로 함께 제공되는지 검증합니다. */
    @Test
    @DisplayName("Mock Dataset은 준비 미완료 캠페인의 조치 후보를 제공한다")
    void createsActionCandidateForIncompletePreparation() {
        AdminOverviewMockDataset dataset = new AdminOverviewMockDataFactory().create(SNAPSHOT_AT);

        assertThat(dataset.actionCandidates())
                .singleElement()
                .satisfies(action -> {
                    assertThat(action.couponId()).isEqualTo(103L);
                    assertThat(action.recommendedAction().code())
                            .isEqualTo(AdminOverviewSnapshot.ActionCode.CAMPAIGN_NOT_READY);
                    assertThat(action.detectedAt()).isEqualTo(SNAPSHOT_AT);
                });
    }
}

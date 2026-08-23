package com.kafkick.core.admin.overview.observation;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.overview.OverviewCalculationPolicy;

/**
 * 하나의 운영현황 Snapshot을 위한 관측 기준 시각과 고유 캠페인 모집단입니다.
 *
 * <p>대상 목록은 생성 시 불변 복사해 조회 중 호출자 변경으로 O1 모집단이 달라지는 일을 막습니다.</p>
 *
 * @param snapshotAt 여러 관측 원천을 같은 운영현황으로 조립할 기준 시각
 * @param campaignTargets O1 관측 대상인 고유 캠페인 목록
 * @param policy O1 연속 감소·중단 조건을 Core 계산기와 같은 기준으로 도출할 정책
 */
public record OverviewObservationRequest(
        Instant snapshotAt,
        List<CampaignObservationTarget> campaignTargets,
        OverviewCalculationPolicy policy
) {

    /** 기준 시각과 중복 없는 캠페인 대상 목록을 검증하고 불변 복사합니다. */
    public OverviewObservationRequest {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        campaignTargets = List.copyOf(Objects.requireNonNull(campaignTargets, "campaignTargets"));
        Objects.requireNonNull(policy, "policy");
        validateUniqueCouponIds(campaignTargets);
    }

    /** 동일 쿠폰을 둘 이상 요청해 O1 모집단 의미가 모호해지는 것을 막습니다. */
    private static void validateUniqueCouponIds(List<CampaignObservationTarget> campaignTargets) {
        Set<Long> couponIds = new HashSet<>();
        for (CampaignObservationTarget campaignTarget : campaignTargets) {
            if (!couponIds.add(campaignTarget.couponId())) {
                throw new IllegalArgumentException("campaignTargets의 couponId는 중복될 수 없습니다.");
            }
        }
    }
}

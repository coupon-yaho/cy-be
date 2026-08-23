package com.kafkick.core.admin.overview.observation;

import java.util.Objects;

import com.kafkick.core.coupon.CouponStatus;

/**
 * O1 관측을 요청할 캠페인의 상태와 발급 중단 판정용 재고 조건입니다.
 *
 * <p>진행 중인 캠페인은 재고가 남았는지를 명시해 O1 중단 판정에 사용합니다. 아직 시작하지 않았거나
 * 종료한 캠페인은 발급 흐름 판정 대상이 아니므로 해당 값을 갖지 않습니다.</p>
 *
 * @param couponId 관측할 캠페인의 양수 쿠폰 식별자
 * @param campaignStatus 요청 시점의 캠페인 상태
 * @param stockAvailable 진행 캠페인의 명시 재고 가능 여부; 그 밖의 상태에서는 null
 */
public record CampaignObservationTarget(
        Long couponId,
        CouponStatus campaignStatus,
        Boolean stockAvailable
) {

    /** 캠페인 식별자와 상태별 재고 가능 여부의 의미 있는 조합만 허용합니다. */
    public CampaignObservationTarget {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(campaignStatus, "campaignStatus");
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        if (campaignStatus == CouponStatus.OPEN && stockAvailable == null) {
            throw new IllegalArgumentException("진행 중인 캠페인은 stockAvailable이 필요합니다.");
        }
        if (campaignStatus != CouponStatus.OPEN && stockAvailable != null) {
            throw new IllegalArgumentException("진행 중이 아닌 캠페인의 stockAvailable은 null이어야 합니다.");
        }
    }
}

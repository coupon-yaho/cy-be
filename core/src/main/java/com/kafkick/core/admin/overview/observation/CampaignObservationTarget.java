package com.kafkick.core.admin.overview.observation;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.coupon.CouponStatus;
import com.kafkick.core.observation.SourceStatus;

/**
 * O1 관측을 요청할 캠페인의 상태와 발급 중단 판정용 재고 관측입니다.
 *
 * <p>진행 중인 캠페인은 재고가 남았는지 또는 아직 값을 얻지 못한 상태를 손실 없이 명시합니다.
 * 아직 시작하지 않았거나 종료한 캠페인은 발급 흐름 판정 대상이 아니므로 N_A/null만 갖습니다.</p>
 *
 * @param couponId 관측할 캠페인의 양수 쿠폰 식별자
 * @param campaignStatus 요청 시점의 캠페인 상태
 * @param stockAvailable 진행 캠페인의 명시 재고 가능 여부; 그 밖의 상태와 값 없는 원천에서는 null
 * @param stockStatus 재고 가능 여부의 명시 원천 상태
 * @param stockObservedAt 값 있는 재고 가능 여부를 실제 관측한 시각; 값 없는 원천과 비진행 상태에서는 null
 */
public record CampaignObservationTarget(
        Long couponId,
        CouponStatus campaignStatus,
        Boolean stockAvailable,
        SourceStatus stockStatus,
        Instant stockObservedAt
) {

    /** 캠페인 식별자와 상태별 재고 값·원천 상태의 canonical 조합만 허용합니다. */
    public CampaignObservationTarget {
        Objects.requireNonNull(couponId, "couponId");
        Objects.requireNonNull(campaignStatus, "campaignStatus");
        Objects.requireNonNull(stockStatus, "stockStatus");
        if (couponId <= 0L) {
            throw new IllegalArgumentException("couponId는 양수여야 합니다.");
        }
        if (campaignStatus == CouponStatus.OPEN) {
            if (stockStatus == SourceStatus.N_A) {
                throw new IllegalArgumentException("진행 중인 캠페인의 stockStatus는 N_A일 수 없습니다.");
            }
            if (stockStatus.carriesValue() != (stockAvailable != null)) {
                throw new IllegalArgumentException("재고 상태와 stockAvailable의 값 존재 여부가 일치해야 합니다.");
            }
            if (stockStatus.carriesValue() != (stockObservedAt != null)) {
                throw new IllegalArgumentException("재고 상태와 stockObservedAt의 값 존재 여부가 일치해야 합니다.");
            }
        } else if (stockStatus != SourceStatus.N_A
                || stockAvailable != null || stockObservedAt != null) {
            throw new IllegalArgumentException("진행 중이 아닌 캠페인의 재고는 N_A/null/null이어야 합니다.");
        }
    }

    /** 값 없는 OPEN과 비진행 N_A 호출자가 관측 시각 없이 같은 의미를 생성하도록 호환합니다. */
    public CampaignObservationTarget(
            Long couponId,
            CouponStatus campaignStatus,
            Boolean stockAvailable,
            SourceStatus stockStatus
    ) {
        this(couponId, campaignStatus, stockAvailable, stockStatus, null);
    }

    /** 이 재고 상태보다 나은 O1 상태인지 판정해 원천 합성 경계의 하한을 제공합니다. */
    public boolean rejectsFlowStatus(SourceStatus flowStatus) {
        Objects.requireNonNull(flowStatus, "flowStatus");
        return switch (stockStatus) {
            case VALID -> false;
            case N_A -> false;
            case STALE -> flowStatus != SourceStatus.STALE
                    && flowStatus != SourceStatus.PENDING
                    && flowStatus != SourceStatus.UNAVAILABLE;
            case WARMING_UP -> flowStatus == SourceStatus.VALID
                    || flowStatus == SourceStatus.NO_TRAFFIC
                    || flowStatus == SourceStatus.N_A;
            case NO_TRAFFIC -> flowStatus == SourceStatus.VALID || flowStatus == SourceStatus.N_A;
            case PENDING -> flowStatus != SourceStatus.PENDING
                    && flowStatus != SourceStatus.UNAVAILABLE;
            case UNAVAILABLE -> flowStatus != SourceStatus.UNAVAILABLE;
        };
    }
}

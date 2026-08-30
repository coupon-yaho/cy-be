package com.kafkick.core.admin.overview.observation;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.kafkick.core.admin.overview.OverviewCalculationPolicy;

/**
 * 하나의 운영현황 Snapshot을 위한 관측 기준 시각과 고유 쿠폰 회차 모집단입니다.
 *
 * <p>대상 목록은 생성 시 불변 복사해 조회 중 호출자 변경으로 O1 모집단이 달라지는 일을 막습니다.</p>
 *
 * @param snapshotAt 여러 관측 원천을 같은 운영현황으로 조립할 기준 시각
 * @param couponRoundTargets O1 관측 대상인 고유 쿠폰 회차 목록
 * @param policy O1 연속 감소·중단 조건을 Core 계산기와 같은 기준으로 도출할 정책
 */
public record OverviewObservationRequest(
        Instant snapshotAt,
        List<CouponRoundObservationTarget> couponRoundTargets,
        OverviewCalculationPolicy policy
) {

    /** 기준 시각과 중복 없는 쿠폰 회차 대상 목록을 검증하고 불변 복사합니다. */
    public OverviewObservationRequest {
        Objects.requireNonNull(snapshotAt, "snapshotAt");
        couponRoundTargets = List.copyOf(Objects.requireNonNull(couponRoundTargets, "couponRoundTargets"));
        Objects.requireNonNull(policy, "policy");
        validateUniqueCouponIds(couponRoundTargets);
        validateStockObservationBoundary(snapshotAt, couponRoundTargets);
    }

    /** 동일 쿠폰을 둘 이상 요청해 O1 모집단 의미가 모호해지는 것을 막습니다. */
    private static void validateUniqueCouponIds(List<CouponRoundObservationTarget> couponRoundTargets) {
        Set<Long> couponIds = new HashSet<>();
        for (CouponRoundObservationTarget couponRoundTarget : couponRoundTargets) {
            if (!couponIds.add(couponRoundTarget.couponId())) {
                throw new IllegalArgumentException("couponRoundTargets의 couponId는 중복될 수 없습니다.");
            }
        }
    }

    /** 값 있는 재고 관측이 요청 Snapshot 이후의 미래 정보를 포함하지 않는지 검증합니다. */
    private static void validateStockObservationBoundary(
            Instant snapshotAt,
            List<CouponRoundObservationTarget> couponRoundTargets
    ) {
        for (CouponRoundObservationTarget couponRoundTarget : couponRoundTargets) {
            if (couponRoundTarget.stockStatus().carriesValue()
                    && couponRoundTarget.stockObservedAt().isAfter(snapshotAt)) {
                throw new IllegalArgumentException("stockObservedAt은 snapshotAt 이후일 수 없습니다.");
            }
        }
    }
}

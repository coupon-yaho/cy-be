package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;

public interface IssuanceRepository {

    /**
     * 같은 쿠폰 회차를 회원이 이미 발급받았는지 조회합니다.
     *
     * @param couponRoundId 쿠폰 회차 식별자
     * @param memberId 회원 식별자
     * @return 기존 발급건이 있으면 {@code true}
     */
    boolean existsForCouponRoundAndMember(
            Long couponRoundId,
            Long memberId
    );

    Issuance save(Issuance issuance);

    Optional<Issuance> findForCouponRoundMemberAndIdempotencyKey(
            Long couponRoundId,
            Long memberId,
            String idempotencyKey
    );

    Optional<Issuance> findById(Long issuanceId);

    boolean updateStatusIfCurrent(
            Long issuanceId,
            Long memberId,
            IssuanceStatus currentStatus,
            IssuanceStatus nextStatus,
            Instant updatedAt
    );
}

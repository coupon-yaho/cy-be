package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupon.query.CouponIssuePolicySnapshot;

public interface CouponRoundRepository {

    CouponRound saveWithInitialStock(
            CouponRound couponRound,
            CouponStock initialStock
    );

    Optional<CouponRound> findById(Long couponRoundId);

    /**
     * 발급 사전검증에 필요한 회차 정책과 1인 1매 여부를 한 번의 조회로 읽습니다.
     *
     * @param couponRoundId 쿠폰 회차 식별자
     * @param memberId 회원 식별자
     * @return 회차가 없으면 빈 값
     */
    Optional<CouponIssuePolicySnapshot> findIssuePolicySnapshot(
            Long couponRoundId,
            Long memberId
    );

    boolean existsByTemplateIdAndOpenAt(Long templateId, Instant openAt);

    boolean existsOverlappingSchedule(Instant openAt, Instant closeAt);
}

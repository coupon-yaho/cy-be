package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;

public interface CouponRoundRepository {

    CouponRound saveWithInitialStock(
            CouponRound couponRound,
            CouponStock initialStock
    );

    Optional<CouponRound> findById(Long couponRoundId);

    boolean existsByTemplateIdAndOpenAt(Long templateId, Instant openAt);

    boolean existsOverlappingSchedule(Instant openAt, Instant closeAt);
}

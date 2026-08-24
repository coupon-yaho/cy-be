package com.kafkick.core.coupon.port;

import java.util.Optional;

import com.kafkick.core.coupon.query.CouponRoundDetail;

public interface CouponRoundDetailQueryPort {

    Optional<CouponRoundDetail> findById(Long couponRoundId);
}

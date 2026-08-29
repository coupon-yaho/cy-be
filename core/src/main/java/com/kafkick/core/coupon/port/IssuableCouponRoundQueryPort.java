package com.kafkick.core.coupon.port;

import java.time.Instant;

import com.kafkick.core.coupon.query.IssuableCouponRoundPage;

public interface IssuableCouponRoundQueryPort {

    IssuableCouponRoundPage findPage(
            Long memberId,
            int membershipGradeBit,
            Instant asOf,
            int page,
            int size
    );
}

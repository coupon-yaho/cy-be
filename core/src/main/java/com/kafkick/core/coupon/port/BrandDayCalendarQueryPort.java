package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupon.query.CouponRoundDetail;

public interface BrandDayCalendarQueryPort {

    List<CouponRoundDetail> findBetween(
            Instant fromInclusive,
            Instant toExclusive
    );
}

package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;

public interface PublicCouponRoundQueryPort {

    PublicCouponRoundPage findPage(
            CouponRoundStatus status,
            int page,
            int size
    );
}

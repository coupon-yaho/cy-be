package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;
import com.kafkick.core.membership.domain.MembershipGrade;

public interface PublicCouponRoundQueryPort {

    PublicCouponRoundPage findPage(
            CouponRoundStatus status,
            MembershipGrade eligibleGrade,
            int page,
            int size
    );
}

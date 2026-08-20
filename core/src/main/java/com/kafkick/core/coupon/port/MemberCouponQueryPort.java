package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.query.MemberCouponPage;

public interface MemberCouponQueryPort {

    MemberCouponPage findPageByMemberId(
            Long memberId,
            IssuanceStatus status,
            int page,
            int size
    );
}

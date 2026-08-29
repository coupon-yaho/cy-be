package com.kafkick.core.coupon.port;

import java.util.Optional;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.core.coupon.query.MemberCouponSummary;

public interface MemberCouponQueryPort {

    MemberCouponPage findPageByMemberId(
            Long memberId,
            IssuanceStatus status,
            int page,
            int size
    );

    Optional<MemberCouponSummary> findByMemberIdAndIssuanceId(
            Long memberId,
            Long issuanceId
    );
}

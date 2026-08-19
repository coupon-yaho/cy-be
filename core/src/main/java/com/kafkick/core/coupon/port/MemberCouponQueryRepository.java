// 회원 소유권과 상태 조건으로 보유 쿠폰 페이지를 조회하는 저장 계약입니다.
package com.kafkick.core.coupon.port;

import com.kafkick.core.coupon.domain.IssuanceStatus;

public interface MemberCouponQueryRepository {

    MemberCouponPage findPageByMemberId(
            Long memberId,
            IssuanceStatus status,
            int page,
            int size
    );
}

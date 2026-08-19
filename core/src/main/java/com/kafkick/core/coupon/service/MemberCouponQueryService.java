// 사용자 보유 쿠폰 목록 조회를 기술 독립적인 포트에 위임합니다.
package com.kafkick.core.coupon.service;

import java.util.Objects;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.port.MemberCouponPage;
import com.kafkick.core.coupon.port.MemberCouponQueryRepository;

public class MemberCouponQueryService {

    private final MemberCouponQueryRepository memberCouponQueryRepository;

    public MemberCouponQueryService(
            MemberCouponQueryRepository memberCouponQueryRepository
    ) {
        this.memberCouponQueryRepository = Objects.requireNonNull(
                memberCouponQueryRepository
        );
    }

    public MemberCouponPage findPage(
            Long memberId,
            IssuanceStatus status,
            int page,
            int size
    ) {
        return memberCouponQueryRepository.findPageByMemberId(
                memberId,
                status,
                page,
                size
        );
    }
}

package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.core.coupon.port.MemberCouponQueryPort;

public class MemberCouponQueryService {

    private final MemberCouponQueryPort memberCouponQueryPort;

    public MemberCouponQueryService(
            MemberCouponQueryPort memberCouponQueryPort
    ) {
        this.memberCouponQueryPort = Objects.requireNonNull(
                memberCouponQueryPort
        );
    }

    @Transactional(readOnly = true)
    public MemberCouponPage findPage(
            Long memberId,
            IssuanceStatus status,
            int page,
            int size
    ) {
        return memberCouponQueryPort.findPageByMemberId(
                memberId,
                status,
                page,
                size
        );
    }
}

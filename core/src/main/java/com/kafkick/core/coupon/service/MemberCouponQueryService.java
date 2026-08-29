package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponQueryErrorCode;
import com.kafkick.core.coupon.port.MemberCouponQueryPort;
import com.kafkick.core.coupon.query.MemberCouponPage;
import com.kafkick.core.coupon.query.MemberCouponSummary;
import com.kafkick.core.support.exception.BusinessException;

@Service
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

    @Transactional(readOnly = true)
    public MemberCouponSummary findOne(Long memberId, Long issuanceId) {
        return memberCouponQueryPort.findByMemberIdAndIssuanceId(
                        memberId,
                        issuanceId
                )
                .orElseThrow(() -> new BusinessException(
                        CouponQueryErrorCode.MEMBER_COUPON_NOT_FOUND
                ));
    }
}

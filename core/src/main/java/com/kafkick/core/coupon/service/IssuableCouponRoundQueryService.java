package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.port.IssuableCouponRoundQueryPort;
import com.kafkick.core.coupon.query.IssuableCouponRoundPage;
import com.kafkick.core.membership.domain.MembershipGrade;

@Service
public class IssuableCouponRoundQueryService {

    private final IssuableCouponRoundQueryPort queryPort;

    public IssuableCouponRoundQueryService(
            IssuableCouponRoundQueryPort queryPort
    ) {
        this.queryPort = Objects.requireNonNull(queryPort);
    }

    @Transactional(readOnly = true)
    public IssuableCouponRoundPage findPage(
            Long memberId,
            MembershipGrade membershipGrade,
            Instant asOf,
            int page,
            int size
    ) {
        Objects.requireNonNull(membershipGrade);
        Objects.requireNonNull(asOf);

        return queryPort.findPage(
                memberId,
                membershipGrade.getBitValue(),
                asOf,
                page,
                size
        );
    }
}

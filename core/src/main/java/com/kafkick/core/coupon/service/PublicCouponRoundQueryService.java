package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRoundStatus;
import com.kafkick.core.coupon.port.PublicCouponRoundQueryPort;
import com.kafkick.core.coupon.query.PublicCouponRoundPage;
import com.kafkick.core.membership.domain.MembershipGrade;

@Service
public class PublicCouponRoundQueryService {

    private final PublicCouponRoundQueryPort queryPort;

    public PublicCouponRoundQueryService(PublicCouponRoundQueryPort queryPort) {
        this.queryPort = Objects.requireNonNull(queryPort);
    }

    @Transactional(readOnly = true)
    public PublicCouponRoundPage findPage(
            CouponRoundStatus status,
            MembershipGrade eligibleGrade,
            int page,
            int size
    ) {
        return queryPort.findPage(status, eligibleGrade, page, size);
    }
}

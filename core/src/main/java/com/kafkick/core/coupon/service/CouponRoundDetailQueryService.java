package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.exception.CouponRoundErrorCode;
import com.kafkick.core.coupon.port.CouponRoundDetailQueryPort;
import com.kafkick.core.coupon.query.CouponRoundDetail;
import com.kafkick.core.support.exception.BusinessException;

@Service
public class CouponRoundDetailQueryService {

    private final CouponRoundDetailQueryPort queryPort;

    public CouponRoundDetailQueryService(CouponRoundDetailQueryPort queryPort) {
        this.queryPort = Objects.requireNonNull(queryPort);
    }

    @Transactional(readOnly = true)
    public CouponRoundDetail findById(Long couponRoundId) {
        return queryPort.findById(couponRoundId)
                .orElseThrow(() -> new BusinessException(
                        CouponRoundErrorCode.COUPON_ROUND_NOT_FOUND
                ));
    }
}

package com.kafkick.core.coupon.service;

import java.util.Objects;

import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.CouponRound;
import com.kafkick.core.coupon.domain.CouponStock;
import com.kafkick.core.coupon.port.CouponRoundRepository;

public class CouponRoundCreationService {

    private final CouponRoundRepository couponRoundRepository;

    public CouponRoundCreationService(
            CouponRoundRepository couponRoundRepository
    ) {
        this.couponRoundRepository = Objects.requireNonNull(
                couponRoundRepository
        );
    }

    @Transactional
    public CouponRound create(
            CouponRound couponRound,
            CouponStock initialStock
    ) {
        return couponRoundRepository.saveWithInitialStock(
                couponRound,
                initialStock
        );
    }
}

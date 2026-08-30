package com.kafkick.api.observation.issuance;

import java.time.Instant;
import java.util.Objects;

import com.kafkick.core.observation.CouponRoundLifecycleRecorder;

/** Connects domain coupon-round close notifications to the bounded meter registry. */
public final class MeterCouponRoundLifecycleRecorder implements CouponRoundLifecycleRecorder {

    private final CouponRoundMeterRegistry couponRoundMeters;

    public MeterCouponRoundLifecycleRecorder(CouponRoundMeterRegistry couponRoundMeters) {
        this.couponRoundMeters = Objects.requireNonNull(couponRoundMeters, "couponRoundMeters");
    }

    @Override
    public void retireCouponRound(long couponId, Instant closedAt) {
        couponRoundMeters.retireCouponRound(couponId, closedAt);
    }
}

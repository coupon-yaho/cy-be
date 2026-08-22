package com.kafkick.batch.coupon.round;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.CouponRoundLifecycleService;
import com.kafkick.core.coupon.service.result.CouponRoundLifecycleResult;
import com.kafkick.core.support.TimeProvider;

@Component
public class CouponRoundLifecycleRunner {

    private final CouponRoundLifecycleService lifecycleService;
    private final TimeProvider timeProvider;

    public CouponRoundLifecycleRunner(
            CouponRoundLifecycleService lifecycleService,
            TimeProvider timeProvider
    ) {
        this.lifecycleService = lifecycleService;
        this.timeProvider = timeProvider;
    }

    public CouponRoundLifecycleResult runOnce() {
        Instant asOf = timeProvider.instant().truncatedTo(ChronoUnit.MICROS);
        return lifecycleService.synchronize(asOf);
    }
}

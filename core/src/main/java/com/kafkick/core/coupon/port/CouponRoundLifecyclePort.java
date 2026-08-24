package com.kafkick.core.coupon.port;

import java.time.Instant;

public interface CouponRoundLifecyclePort {

    int closeOpenRounds(Instant asOf);

    int closeMissedScheduledRounds(Instant asOf);

    int openScheduledRounds(Instant asOf);
}

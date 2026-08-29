package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.List;

public interface CouponRoundLifecyclePort {

    List<Long> closeOpenRounds(Instant asOf);

    int closeMissedScheduledRounds(Instant asOf);

    int openScheduledRounds(Instant asOf);
}

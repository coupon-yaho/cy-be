package com.kafkick.core.observation;

import java.time.Instant;
import java.util.List;

public interface ClosedCouponRoundRecoverySource {

    List<ClosedCouponRound> findRecentlyClosed(
            Instant fromInclusive,
            Instant toInclusive,
            int limit
    );
}

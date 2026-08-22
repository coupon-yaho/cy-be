package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.List;

import com.kafkick.core.coupon.domain.Issuance;

public interface CouponExpirationCandidateQueryPort {

    List<Issuance> findExpiredIssuedAfterId(
            Instant asOf,
            Long afterId,
            int limit
    );
}

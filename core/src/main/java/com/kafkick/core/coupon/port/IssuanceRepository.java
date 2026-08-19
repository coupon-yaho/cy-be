package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.Optional;
import java.util.List;

import com.kafkick.core.coupon.domain.Issuance;
import com.kafkick.core.coupon.domain.IssuanceStatus;

public interface IssuanceRepository {

    Issuance save(Issuance issuance);

    Optional<Issuance> findById(Long issuanceId);

    List<Issuance> findExpiredIssuedAfterId(
            Instant asOf,
            Long afterId,
            int limit
    );

    boolean updateStatusIfCurrent(
            Long issuanceId,
            Long memberId,
            IssuanceStatus currentStatus,
            IssuanceStatus nextStatus,
            Instant updatedAt
    );
}

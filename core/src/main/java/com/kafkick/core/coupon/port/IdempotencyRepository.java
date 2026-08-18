package com.kafkick.core.coupon.port;

import java.time.Instant;
import java.util.Optional;

import com.kafkick.core.coupon.domain.IdempotencyRecord;

public interface IdempotencyRepository {

    boolean tryStart(
            String key,
            String requestHash,
            Instant createdAt
    );

    Optional<IdempotencyRecord> findByKey(String key);

    boolean tryReclaim(
            String key,
            String requestHash,
            Instant previousClaimedAt,
            Instant reclaimedAt
    );

    void complete(
            String key,
            Long memberId,
            Long issuanceId,
            String responseBody,
            Instant claimedAt
    );

    void release(
            String key,
            String requestHash,
            Instant claimedAt
    );
}

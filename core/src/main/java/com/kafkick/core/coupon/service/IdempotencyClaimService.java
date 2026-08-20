package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.port.IdempotencyRepository;

public class IdempotencyClaimService {

    private final IdempotencyRepository idempotencyRepository;

    public IdempotencyClaimService(
            IdempotencyRepository idempotencyRepository
    ) {
        this.idempotencyRepository = Objects.requireNonNull(
                idempotencyRepository
        );
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryStart(
            String key,
            String requestHash,
            Instant claimedAt
    ) {
        return idempotencyRepository.tryStart(key, requestHash, claimedAt);
    }

    @Transactional(
            propagation = Propagation.REQUIRES_NEW,
            readOnly = true
    )
    public Optional<IdempotencyRecord> findByKey(String key) {
        return idempotencyRepository.findByKey(key);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryReclaim(
            String key,
            String requestHash,
            Instant previousClaimedAt,
            Instant reclaimedAt
    ) {
        return idempotencyRepository.tryReclaim(
                key,
                requestHash,
                previousClaimedAt,
                reclaimedAt
        );
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void release(
            String key,
            String requestHash,
            Instant claimedAt
    ) {
        idempotencyRepository.release(key, requestHash, claimedAt);
    }
}

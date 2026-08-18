// 멱등키 선점·조회·회수를 쿠폰 사용 트랜잭션과 분리된 짧은 트랜잭션으로 처리합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Instant;
import java.util.Optional;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.port.IdempotencyRepository;

@Component
public class IdempotencyClaimTransactionalAdapter {

    private final IdempotencyRepository idempotencyRepository;

    public IdempotencyClaimTransactionalAdapter(
            IdempotencyRepository idempotencyRepository
    ) {
        this.idempotencyRepository = idempotencyRepository;
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

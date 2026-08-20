package com.kafkick.core.coupon.service;

import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;

public class IdempotentOperationService {

    private final IdempotencyRepository idempotencyRepository;

    public IdempotentOperationService(
            IdempotencyRepository idempotencyRepository
    ) {
        this.idempotencyRepository = Objects.requireNonNull(
                idempotencyRepository
        );
    }

    @Transactional
    public <R> R execute(
            String idempotencyKey,
            Long memberId,
            Long issuanceId,
            Instant claimedAt,
            Supplier<R> operation,
            IdempotencyResultCodec<R> resultCodec
    ) {
        R result = operation.get();
        idempotencyRepository.complete(
                idempotencyKey,
                memberId,
                issuanceId,
                resultCodec.write(result),
                claimedAt
        );
        return result;
    }
}

package com.kafkick.core.coupon.service.idempotency;

import java.time.Instant;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;

@Service
public class IdempotentOperationService {

    private final IdempotencyRepository idempotencyRepository;

    public IdempotentOperationService(
            IdempotencyRepository idempotencyRepository
    ) {
        this.idempotencyRepository = Objects.requireNonNull(
                idempotencyRepository
        );
    }

    /**
     * 권위 작업과 완성된 DONE 레코드 INSERT를 한 트랜잭션에서 실행합니다.
     *
     * <p>발급 경로 전용입니다. IN_PROGRESS 선점이 없으므로 실패는 롤백만으로 정리되고
     * {@code release}가 필요하지 않습니다. 같은 멱등키가 이미 있으면 결과를 버리고
     * {@code false}를 담아 돌려주어, 호출부가 저장된 응답 재사용으로 넘어가게 합니다.
     *
     * @return 작업 결과와 기록 성공 여부
     */
    @Transactional
    public <R> RecordedExecution<R> executeAndRecord(
            String idempotencyKey,
            Long memberId,
            String requestHash,
            Instant claimedAt,
            Supplier<R> operation,
            IdempotencyResultCodec<R> resultCodec,
            Function<R, Long> issuanceIdExtractor
    ) {
        R result = operation.get();
        Long issuanceId = Objects.requireNonNull(
                issuanceIdExtractor.apply(result),
                "issuanceId"
        );
        boolean recorded = idempotencyRepository.insertCompleted(
                idempotencyKey,
                memberId,
                issuanceId,
                requestHash,
                resultCodec.write(result),
                claimedAt
        );
        if (!recorded) {
            throw new IdempotencyKeyTakenException(idempotencyKey);
        }
        return new RecordedExecution<>(result);
    }

    /** 한 트랜잭션에서 기록까지 마친 작업 결과입니다. */
    public record RecordedExecution<R>(R result) {
    }

    @Transactional
    public <R> R execute(
            String idempotencyKey,
            Long memberId,
            Instant claimedAt,
            Supplier<R> operation,
            IdempotencyResultCodec<R> resultCodec,
            Function<R, Long> issuanceIdExtractor
    ) {
        R result = operation.get();
        Long issuanceId = Objects.requireNonNull(
                issuanceIdExtractor.apply(result),
                "issuanceId"
        );
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

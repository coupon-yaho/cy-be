package com.kafkick.core.coupon.service.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

@Service
public class IdempotencyExecutionService {

    private static final Pattern UUID_V4_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-"
                    + "[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    );

    private final IdempotencyClaimService claimService;
    private final TimeProvider timeProvider;
    private final IdempotencyPolicy policy;

    @Autowired
    public IdempotencyExecutionService(
            IdempotencyClaimService claimService,
            TimeProvider timeProvider,
            @Value("${coupon.idempotency.wait-timeout}")
            Duration waitTimeout,
            @Value("${coupon.idempotency.poll-interval}")
            Duration pollInterval,
            @Value("${coupon.idempotency.stale-after}")
            Duration staleAfter
    ) {
        this(
                claimService,
                timeProvider,
                new IdempotencyPolicy(
                        waitTimeout,
                        pollInterval,
                        staleAfter
                )
        );
    }

    public IdempotencyExecutionService(
            IdempotencyClaimService claimService,
            TimeProvider timeProvider,
            IdempotencyPolicy policy
    ) {
        this.claimService = claimService;
        this.timeProvider = timeProvider;
        this.policy = policy;
    }

    @Transactional(propagation = Propagation.NEVER)
    public <R> R execute(
            String idempotencyKey,
            Supplier<String> canonicalRequestSupplier,
            ErrorCode invalidRequestErrorCode,
            Function<Instant, R> claimedRequest,
            Function<String, R> completedResponseReader
    ) {
        return executeWithMetadata(
                idempotencyKey,
                canonicalRequestSupplier,
                invalidRequestErrorCode,
                claimedRequest,
                completedResponseReader
        ).value();
    }

    /**
     * 외부 트랜잭션 없이 멱등 실행 값과 DONE 응답 재사용 여부를 함께 반환합니다.
     *
     * @param idempotencyKey UUID v4 멱등 키
     * @param canonicalRequestSupplier 요청 동등성을 판단할 정규화 문자열 공급자
     * @param invalidRequestErrorCode 잘못된 멱등 키에 사용할 업무 오류 코드
     * @param claimedRequest 최초 선점 또는 stale 회수 뒤 실행할 작업
     * @param completedResponseReader 저장된 DONE 응답 복원기
     * @param <R> 실행 결과 타입
     * @return 실행 값과 DONE replay 여부
     */
    @Transactional(propagation = Propagation.NEVER)
    public <R> IdempotentExecutionResult<R> executeWithMetadata(
            String idempotencyKey,
            Supplier<String> canonicalRequestSupplier,
            ErrorCode invalidRequestErrorCode,
            Function<Instant, R> claimedRequest,
            Function<String, R> completedResponseReader
    ) {
        validateIdempotencyKey(idempotencyKey, invalidRequestErrorCode);
        String requestHash = hashRequest(canonicalRequestSupplier.get());
        Instant requestAt = currentTime();
        boolean firstRequest = claimService.tryStart(
                idempotencyKey,
                requestHash,
                requestAt
        );
        if (firstRequest) {
            R value = processClaimedRequest(
                    idempotencyKey,
                    requestHash,
                    requestAt,
                    claimedRequest
            );
            return new IdempotentExecutionResult<>(value, false);
        }
        return replayOrReclaim(
                idempotencyKey,
                requestHash,
                requestAt,
                claimedRequest,
                completedResponseReader
        );
    }

    private <R> IdempotentExecutionResult<R> replayOrReclaim(
            String idempotencyKey,
            String requestHash,
            Instant requestAt,
            Function<Instant, R> claimedRequest,
            Function<String, R> completedResponseReader
    ) {
        long deadline = System.nanoTime() + policy.waitTimeout().toNanos();

        while (true) {
            IdempotencyRecord record = claimService.findByKey(idempotencyKey)
                    .orElseThrow(() -> conflictInProgress(idempotencyKey));
            validateRequestHash(record, requestHash, idempotencyKey);
            if (record.status() == IdempotencyStatus.DONE) {
                return new IdempotentExecutionResult<>(
                        completedResponseReader.apply(record.responseBody()),
                        true
                );
            }
            if (isStale(record, requestAt)
                    && claimService.tryReclaim(
                            idempotencyKey,
                            requestHash,
                            record.createdAt(),
                            requestAt
                    )) {
                R value = processClaimedRequest(
                        idempotencyKey,
                        requestHash,
                        requestAt,
                        claimedRequest
                );
                return new IdempotentExecutionResult<>(value, false);
            }
            if (System.nanoTime() >= deadline) {
                throw conflictInProgress(idempotencyKey);
            }
            pauseBeforeRetry(policy.pollInterval());
        }
    }

    private <R> R processClaimedRequest(
            String idempotencyKey,
            String requestHash,
            Instant requestAt,
            Function<Instant, R> claimedRequest
    ) {
        try {
            return claimedRequest.apply(requestAt);
        } catch (RuntimeException processingException) {
            releaseFailedClaim(
                    idempotencyKey,
                    requestHash,
                    requestAt,
                    processingException
            );
            throw processingException;
        }
    }

    private void releaseFailedClaim(
            String idempotencyKey,
            String requestHash,
            Instant claimedAt,
            RuntimeException processingException
    ) {
        try {
            claimService.release(idempotencyKey, requestHash, claimedAt);
        } catch (RuntimeException cleanupException) {
            processingException.addSuppressed(cleanupException);
        }
    }

    private boolean isStale(IdempotencyRecord record, Instant requestAt) {
        Instant staleBoundary = requestAt.minus(policy.staleAfter());
        return !record.createdAt().isAfter(staleBoundary);
    }

    private static void validateRequestHash(
            IdempotencyRecord record,
            String requestHash,
            String idempotencyKey
    ) {
        if (!record.requestHash().equals(requestHash)) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "idempotencyKey=" + idempotencyKey
            );
        }
    }

    private static BusinessException conflictInProgress(
            String idempotencyKey
    ) {
        return new BusinessException(
                CouponUseErrorCode.CONFLICT_IN_PROGRESS,
                "idempotencyKey=" + idempotencyKey
        );
    }

    private static void validateIdempotencyKey(
            String idempotencyKey,
            ErrorCode invalidRequestErrorCode
    ) {
        if (idempotencyKey == null
                || !UUID_V4_PATTERN.matcher(idempotencyKey).matches()) {
            throw new BusinessException(
                    invalidRequestErrorCode,
                    "Idempotency-Key must be UUID v4"
            );
        }
    }

    private Instant currentTime() {
        return timeProvider.instant().truncatedTo(ChronoUnit.MICROS);
    }

    private static void pauseBeforeRetry(Duration interval) {
        try {
            TimeUnit.NANOSECONDS.sleep(interval.toNanos());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new BusinessException(
                    CouponUseErrorCode.CONFLICT_IN_PROGRESS,
                    "멱등 요청 완료 대기가 중단되었습니다.",
                    exception
            );
        }
    }

    private static String hashRequest(String canonicalRequest) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(
                    canonicalRequest.getBytes(StandardCharsets.UTF_8)
            ));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 알고리즘을 사용할 수 없습니다.",
                    exception
            );
        }
    }
}

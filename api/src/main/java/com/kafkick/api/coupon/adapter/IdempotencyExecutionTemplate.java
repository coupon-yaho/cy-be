// 쿠폰 API의 멱등 선점·재조회·재선점·실패 해제 흐름을 공통으로 실행합니다.
package com.kafkick.api.coupon.adapter;

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

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.config.CouponIdempotencyProperties;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.ErrorCode;

@Component
public class IdempotencyExecutionTemplate {

    private static final Pattern UUID_V4_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-"
                    + "[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    );

    private final IdempotencyClaimTransactionalAdapter claimAdapter;
    private final TimeProvider timeProvider;
    private final CouponIdempotencyProperties properties;

    public IdempotencyExecutionTemplate(
            IdempotencyClaimTransactionalAdapter claimAdapter,
            TimeProvider timeProvider,
            CouponIdempotencyProperties properties
    ) {
        this.claimAdapter = claimAdapter;
        this.timeProvider = timeProvider;
        this.properties = properties;
    }

    public <R> R execute(
            String idempotencyKey,
            Supplier<String> canonicalRequestSupplier,
            ErrorCode invalidRequestErrorCode,
            Function<Instant, R> claimedRequest,
            Function<String, R> completedResponseReader
    ) {
        validateIdempotencyKey(idempotencyKey, invalidRequestErrorCode);
        String requestHash = hashRequest(canonicalRequestSupplier.get());
        Instant requestAt = currentTime();
        boolean firstRequest = claimAdapter.tryStart(
                idempotencyKey,
                requestHash,
                requestAt
        );
        if (firstRequest) {
            return processClaimedRequest(
                    idempotencyKey,
                    requestHash,
                    requestAt,
                    claimedRequest
            );
        }
        return replayOrReclaim(
                idempotencyKey,
                requestHash,
                requestAt,
                claimedRequest,
                completedResponseReader
        );
    }

    private <R> R replayOrReclaim(
            String idempotencyKey,
            String requestHash,
            Instant requestAt,
            Function<Instant, R> claimedRequest,
            Function<String, R> completedResponseReader
    ) {
        long deadline = System.nanoTime()
                + properties.waitTimeout().toNanos();

        while (true) {
            IdempotencyRecord record = claimAdapter
                    .findByKey(idempotencyKey)
                    .orElseThrow(() -> conflictInProgress(idempotencyKey));
            validateRequestHash(record, requestHash, idempotencyKey);
            if (record.status() == IdempotencyStatus.DONE) {
                return completedResponseReader.apply(record.responseBody());
            }
            if (isStale(record, requestAt)
                    && claimAdapter.tryReclaim(
                            idempotencyKey,
                            requestHash,
                            record.createdAt(),
                            requestAt
                    )) {
                return processClaimedRequest(
                        idempotencyKey,
                        requestHash,
                        requestAt,
                        claimedRequest
                );
            }
            if (System.nanoTime() >= deadline) {
                throw conflictInProgress(idempotencyKey);
            }
            pauseBeforeRetry(properties.pollInterval());
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
            claimAdapter.release(idempotencyKey, requestHash, claimedAt);
        } catch (RuntimeException cleanupException) {
            processingException.addSuppressed(cleanupException);
        }
    }

    private boolean isStale(
            IdempotencyRecord record,
            Instant requestAt
    ) {
        Instant staleBoundary = requestAt.minus(properties.staleAfter());
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
            throw invalidIdempotencyKey(invalidRequestErrorCode);
        }
    }

    private static BusinessException invalidIdempotencyKey(
            ErrorCode invalidRequestErrorCode
    ) {
        return new BusinessException(
                invalidRequestErrorCode,
                "Idempotency-Key must be UUID v4"
        );
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

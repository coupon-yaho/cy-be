// 멱등 선점을 짧게 커밋하고 처리 중 요청을 제한 재조회한 뒤 쿠폰 사용 트랜잭션을 실행합니다.
package com.kafkick.api.coupon.adapter;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.config.CouponIdempotencyProperties;
import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.CouponUseCommand;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

@Component
public class CouponUseTransactionalAdapter {

    private static final Pattern UUID_V4_PATTERN = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-"
                    + "[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    );

    private final IdempotencyClaimTransactionalAdapter claimAdapter;
    private final CouponUseTransactionExecutor transactionExecutor;
    private final CouponUseResponseCodec responseCodec;
    private final TimeProvider timeProvider;
    private final CouponIdempotencyProperties properties;

    public CouponUseTransactionalAdapter(
            IdempotencyClaimTransactionalAdapter claimAdapter,
            CouponUseTransactionExecutor transactionExecutor,
            CouponUseResponseCodec responseCodec,
            TimeProvider timeProvider,
            CouponIdempotencyProperties properties
    ) {
        this.claimAdapter = claimAdapter;
        this.transactionExecutor = transactionExecutor;
        this.responseCodec = responseCodec;
        this.timeProvider = timeProvider;
        this.properties = properties;
    }

    public CouponUseResponse use(
            Long issuanceId,
            Long memberId,
            String idempotencyKey,
            CouponUseRequest request
    ) {
        validateIdempotencyKey(idempotencyKey);
        String requestHash = hashRequest(issuanceId, memberId, request);
        Instant requestAt = currentTime();
        boolean firstRequest = claimAdapter.tryStart(
                idempotencyKey,
                requestHash,
                requestAt
        );
        if (firstRequest) {
            return processClaimedRequest(
                    issuanceId,
                    memberId,
                    idempotencyKey,
                    requestHash,
                    request,
                    requestAt
            );
        }
        return replayOrReclaim(
                issuanceId,
                memberId,
                idempotencyKey,
                requestHash,
                request,
                requestAt
        );
    }

    private CouponUseResponse replayOrReclaim(
            Long issuanceId,
            Long memberId,
            String idempotencyKey,
            String requestHash,
            CouponUseRequest request,
            Instant requestAt
    ) {
        long deadline = System.nanoTime()
                + properties.waitTimeout().toNanos();

        while (true) {
            IdempotencyRecord record = claimAdapter
                    .findByKey(idempotencyKey)
                    .orElseThrow(() -> conflictInProgress(idempotencyKey));
            validateRequestHash(record, requestHash, idempotencyKey);
            if (record.status() == IdempotencyStatus.DONE) {
                return responseCodec.read(record.responseBody());
            }
            if (isStale(record, requestAt)
                    && claimAdapter.tryReclaim(
                            idempotencyKey,
                            requestHash,
                            record.createdAt(),
                            requestAt
                    )) {
                return processClaimedRequest(
                        issuanceId,
                        memberId,
                        idempotencyKey,
                        requestHash,
                        request,
                        requestAt
                );
            }
            if (System.nanoTime() >= deadline) {
                throw conflictInProgress(idempotencyKey);
            }
            pauseBeforeRetry(properties.pollInterval());
        }
    }

    private CouponUseResponse processClaimedRequest(
            Long issuanceId,
            Long memberId,
            String idempotencyKey,
            String requestHash,
            CouponUseRequest request,
            Instant requestAt
    ) {
        try {
            return transactionExecutor.execute(
                    new CouponUseCommand(
                            issuanceId,
                            memberId,
                            request.orderId(),
                            request.orderAmount(),
                            idempotencyKey,
                            requestAt
                    ),
                    requestAt
            );
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

    private static void validateIdempotencyKey(String idempotencyKey) {
        if (idempotencyKey == null
                || !UUID_V4_PATTERN.matcher(idempotencyKey).matches()) {
            throw invalidIdempotencyKey();
        }
        UUID uuid = UUID.fromString(idempotencyKey);
        if (uuid.version() != 4) {
            throw invalidIdempotencyKey();
        }
    }

    private static BusinessException invalidIdempotencyKey() {
        return new BusinessException(
                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
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

    private static String hashRequest(
            Long issuanceId,
            Long memberId,
            CouponUseRequest request
    ) {
        String canonical = "USE|issuanceId=" + issuanceId
                + "|memberId=" + memberId
                + "|orderId=" + request.orderId()
                + "|orderAmount=" + request.orderAmount();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(
                    digest.digest(canonical.getBytes(StandardCharsets.UTF_8))
            );
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 알고리즘을 사용할 수 없습니다.",
                    exception
            );
        }
    }
}

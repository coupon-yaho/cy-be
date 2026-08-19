// 쿠폰 API가 공유하는 멱등 선점·실행 흐름과 요청별 전략 주입을 검증합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.api.coupon.config.CouponIdempotencyProperties;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyExecutionTemplateTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final String CANONICAL_REQUEST =
            "USE|issuanceId=100|memberId=20|orderId=30|orderAmount=20000";
    private static final String REQUEST_HASH =
            "52c137166d55b7c4891c4636585ecb7ca7d753d0d1c86852a978a7a3cb82d93e";
    private static final Instant REQUEST_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IdempotencyClaimTransactionalAdapter claimAdapter;

    @Mock
    private TimeProvider timeProvider;

    private IdempotencyExecutionTemplate template;

    @BeforeEach
    void setUp() {
        template = new IdempotencyExecutionTemplate(
                claimAdapter,
                timeProvider,
                new CouponIdempotencyProperties(
                        Duration.ofMillis(100),
                        Duration.ofMillis(1),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    @DisplayName("정규화 요청을 공통 해시로 선점하고 같은 시각으로 실행 콜백을 호출한다")
    void claimCanonicalRequestAndExecuteWithOneTimestamp() {
        when(timeProvider.instant()).thenReturn(REQUEST_AT);
        when(claimAdapter.tryStart(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                REQUEST_AT
        )).thenReturn(true);
        AtomicReference<Instant> executedAt = new AtomicReference<>();

        String result = template.execute(
                IDEMPOTENCY_KEY,
                () -> CANONICAL_REQUEST,
                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                claimedAt -> {
                    executedAt.set(claimedAt);
                    return "processed";
                },
                storedResponse -> "replayed"
        );

        assertThat(result).isEqualTo("processed");
        assertThat(executedAt).hasValue(REQUEST_AT);
        verify(timeProvider).instant();
    }

    @Test
    @DisplayName("처리 중인 멱등 요청이 대기 제한을 넘으면 충돌로 응답한다")
    void rejectWhenInProgressRequestExceedsWaitTimeout() {
        template = templateWithWaitTimeout(Duration.ZERO);
        IdempotencyRecord inProgress = inProgressRecord(REQUEST_AT);
        when(timeProvider.instant()).thenReturn(REQUEST_AT);
        when(claimAdapter.tryStart(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                REQUEST_AT
        )).thenReturn(false);
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenReturn(Optional.of(inProgress));

        BusinessException exception = executeAndCatchConflict();

        assertThat(exception.getErrorCode())
                .isEqualTo(CouponUseErrorCode.CONFLICT_IN_PROGRESS);
        verify(claimAdapter, never()).tryReclaim(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                REQUEST_AT,
                REQUEST_AT
        );
    }

    @Test
    @DisplayName("만료된 선점의 재선점 경합에서 패하면 처리 중 충돌로 응답한다")
    void rejectWhenStaleClaimReclaimLosesRace() {
        template = templateWithWaitTimeout(Duration.ZERO);
        Instant staleClaimedAt = REQUEST_AT.minusSeconds(31);
        IdempotencyRecord inProgress = inProgressRecord(staleClaimedAt);
        when(timeProvider.instant()).thenReturn(REQUEST_AT);
        when(claimAdapter.tryStart(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                REQUEST_AT
        )).thenReturn(false);
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenReturn(Optional.of(inProgress));
        when(claimAdapter.tryReclaim(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                staleClaimedAt,
                REQUEST_AT
        )).thenReturn(false);

        BusinessException exception = executeAndCatchConflict();

        assertThat(exception.getErrorCode())
                .isEqualTo(CouponUseErrorCode.CONFLICT_IN_PROGRESS);
        verify(claimAdapter).tryReclaim(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                staleClaimedAt,
                REQUEST_AT
        );
    }

    @Test
    @DisplayName("처리 실패 후 선점 해제도 실패하면 정리 예외를 원인 예외에 보존한다")
    void preserveReleaseFailureAsSuppressedException() {
        RuntimeException processingFailure =
                new RuntimeException("processing failed");
        RuntimeException releaseFailure =
                new RuntimeException("release failed");
        when(timeProvider.instant()).thenReturn(REQUEST_AT);
        when(claimAdapter.tryStart(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                REQUEST_AT
        )).thenReturn(true);
        doThrow(releaseFailure)
                .when(claimAdapter)
                .release(IDEMPOTENCY_KEY, REQUEST_HASH, REQUEST_AT);

        RuntimeException exception = catchThrowableOfType(
                RuntimeException.class,
                () -> template.execute(
                        IDEMPOTENCY_KEY,
                        () -> CANONICAL_REQUEST,
                        CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                        claimedAt -> {
                            throw processingFailure;
                        },
                        storedResponse -> "replayed"
                )
        );

        assertThat(exception).isSameAs(processingFailure);
        assertThat(exception.getSuppressed()).containsExactly(releaseFailure);
    }

    @Test
    @DisplayName("선점 실패 후 멱등 레코드를 찾지 못하면 처리 중 충돌로 응답한다")
    void rejectWhenClaimedRecordCannotBeFound() {
        when(timeProvider.instant()).thenReturn(REQUEST_AT);
        when(claimAdapter.tryStart(
                IDEMPOTENCY_KEY,
                REQUEST_HASH,
                REQUEST_AT
        )).thenReturn(false);
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenReturn(Optional.empty());

        BusinessException exception = executeAndCatchConflict();

        assertThat(exception.getErrorCode())
                .isEqualTo(CouponUseErrorCode.CONFLICT_IN_PROGRESS);
    }

    private BusinessException executeAndCatchConflict() {
        return catchThrowableOfType(
                BusinessException.class,
                () -> template.execute(
                        IDEMPOTENCY_KEY,
                        () -> CANONICAL_REQUEST,
                        CouponUseErrorCode.INVALID_COUPON_USE_REQUEST,
                        claimedAt -> "processed",
                        storedResponse -> "replayed"
                )
        );
    }

    private IdempotencyExecutionTemplate templateWithWaitTimeout(
            Duration waitTimeout
    ) {
        return new IdempotencyExecutionTemplate(
                claimAdapter,
                timeProvider,
                new CouponIdempotencyProperties(
                        waitTimeout,
                        Duration.ofMillis(1),
                        Duration.ofSeconds(30)
                )
        );
    }

    private static IdempotencyRecord inProgressRecord(Instant createdAt) {
        return new IdempotencyRecord(
                IDEMPOTENCY_KEY,
                null,
                null,
                REQUEST_HASH,
                IdempotencyStatus.IN_PROGRESS,
                null,
                createdAt
        );
    }
}

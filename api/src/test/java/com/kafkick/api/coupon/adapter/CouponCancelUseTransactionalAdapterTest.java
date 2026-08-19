// 쿠폰 사용 취소의 멱등 선점·응답 재생·오래된 선점 회수를 검증합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.coupon.config.CouponIdempotencyProperties;
import com.kafkick.api.coupon.dto.CouponCancelUseResponse;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.CouponCancelUseCommand;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponCancelUseTransactionalAdapterTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant CANCELED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IdempotencyClaimTransactionalAdapter claimAdapter;

    @Mock
    private CouponCancelUseTransactionExecutor transactionExecutor;

    @Mock
    private TimeProvider timeProvider;

    private CouponCancelUseResponseCodec responseCodec;
    private CouponCancelUseTransactionalAdapter adapter;

    @BeforeEach
    void setUp() {
        responseCodec = new CouponCancelUseResponseCodec(new ObjectMapper());
        IdempotencyExecutionTemplate idempotencyTemplate =
                new IdempotencyExecutionTemplate(
                        claimAdapter,
                        timeProvider,
                        new CouponIdempotencyProperties(
                                Duration.ofMillis(100),
                                Duration.ofMillis(1),
                                Duration.ofSeconds(30)
                        )
                );
        adapter = new CouponCancelUseTransactionalAdapter(
                idempotencyTemplate,
                transactionExecutor,
                responseCodec
        );
    }

    @Test
    @DisplayName("최초 요청은 한 번 생성한 시각으로 선점하고 사용 취소를 실행한다")
    void processFirstRequestWithOneTimestamp() {
        when(timeProvider.instant()).thenReturn(CANCELED_AT);
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(CANCELED_AT)
        )).thenReturn(true);
        when(transactionExecutor.execute(any(), eq(CANCELED_AT)))
                .thenReturn(response());

        CouponCancelUseResponse actual = adapter.cancelUse(
                100L,
                20L,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(response());
        ArgumentCaptor<CouponCancelUseCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponCancelUseCommand.class);
        verify(transactionExecutor).execute(
                commandCaptor.capture(),
                eq(CANCELED_AT)
        );
        assertThat(commandCaptor.getValue().issuanceId()).isEqualTo(100L);
        assertThat(commandCaptor.getValue().memberId()).isEqualTo(20L);
        assertThat(commandCaptor.getValue().idempotencyKey())
                .isEqualTo(IDEMPOTENCY_KEY);
        assertThat(commandCaptor.getValue().canceledAt())
                .isEqualTo(CANCELED_AT);
        verify(timeProvider).instant();
    }

    @Test
    @DisplayName("DONE 요청은 실제 JSON으로 저장된 최초 사용 취소 응답을 반환한다")
    void replayCompletedRequest() {
        when(timeProvider.instant()).thenReturn(CANCELED_AT);
        AtomicReference<String> requestHash = new AtomicReference<>();
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(CANCELED_AT)
        )).thenAnswer(invocation -> {
            requestHash.set(invocation.getArgument(1));
            return false;
        });
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenAnswer(invocation -> Optional.of(record(
                        requestHash.get(),
                        IdempotencyStatus.DONE,
                        responseCodec.write(response()),
                        CANCELED_AT
                )));

        CouponCancelUseResponse actual = adapter.cancelUse(
                100L,
                20L,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(response());
        verify(transactionExecutor, never()).execute(any(), any());
    }

    @Test
    @DisplayName("오래된 IN_PROGRESS 요청을 조건부 회수해 다시 처리한다")
    void reclaimStaleRequest() {
        when(timeProvider.instant()).thenReturn(CANCELED_AT);
        Instant oldClaimedAt = CANCELED_AT.minusSeconds(31);
        AtomicReference<String> requestHash = new AtomicReference<>();
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(CANCELED_AT)
        )).thenAnswer(invocation -> {
            requestHash.set(invocation.getArgument(1));
            return false;
        });
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenAnswer(invocation -> Optional.of(record(
                        requestHash.get(),
                        IdempotencyStatus.IN_PROGRESS,
                        null,
                        oldClaimedAt
                )));
        when(claimAdapter.tryReclaim(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(oldClaimedAt),
                eq(CANCELED_AT)
        )).thenReturn(true);
        when(transactionExecutor.execute(any(), eq(CANCELED_AT)))
                .thenReturn(response());

        assertThat(adapter.cancelUse(100L, 20L, IDEMPOTENCY_KEY))
                .isEqualTo(response());
        verify(transactionExecutor).execute(any(), eq(CANCELED_AT));
    }

    @Test
    @DisplayName("다른 요청의 멱등키를 재사용하면 422를 반환한다")
    void rejectReusedKeyForDifferentRequest() {
        when(timeProvider.instant()).thenReturn(CANCELED_AT);
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(CANCELED_AT)
        )).thenReturn(false);
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenReturn(Optional.of(record(
                        "f".repeat(64),
                        IdempotencyStatus.DONE,
                        responseCodec.write(response()),
                        CANCELED_AT
                )));

        assertErrorCode(
                () -> adapter.cancelUse(100L, 20L, IDEMPOTENCY_KEY),
                CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED
        );
    }

    @Test
    @DisplayName("사용 취소 실패 시 자신이 선점한 멱등 레코드를 해제한다")
    void releaseClaimAfterFailure() {
        when(timeProvider.instant()).thenReturn(CANCELED_AT);
        AtomicReference<String> requestHash = new AtomicReference<>();
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(CANCELED_AT)
        )).thenAnswer(invocation -> {
            requestHash.set(invocation.getArgument(1));
            return true;
        });
        BusinessException failure = new BusinessException(
                CouponUseErrorCode.ACTIVE_USAGE_NOT_FOUND
        );
        when(transactionExecutor.execute(any(), eq(CANCELED_AT)))
                .thenThrow(failure);

        assertThatThrownBy(() ->
                adapter.cancelUse(100L, 20L, IDEMPOTENCY_KEY)
        ).isSameAs(failure);
        verify(claimAdapter).release(
                IDEMPOTENCY_KEY,
                requestHash.get(),
                CANCELED_AT
        );
    }

    @Test
    @DisplayName("UUID v4가 아닌 멱등키는 선점 전에 거부한다")
    void rejectInvalidIdempotencyKey() {
        assertErrorCode(
                () -> adapter.cancelUse(100L, 20L, "invalid-key"),
                CouponUseErrorCode.INVALID_COUPON_CANCEL_USE_REQUEST
        );
        verify(claimAdapter, never()).tryStart(any(), any(), any());
        verify(timeProvider, never()).instant();
    }

    private IdempotencyRecord record(
            String requestHash,
            IdempotencyStatus status,
            String responseBody,
            Instant createdAt
    ) {
        return new IdempotencyRecord(
                IDEMPOTENCY_KEY,
                status == IdempotencyStatus.DONE ? 20L : null,
                status == IdempotencyStatus.DONE ? 100L : null,
                requestHash,
                status,
                responseBody,
                createdAt
        );
    }

    private CouponCancelUseResponse response() {
        return new CouponCancelUseResponse(
                100L,
                IssuanceStatus.ISSUED,
                30L,
                5_000,
                CANCELED_AT
        );
    }

    private void assertErrorCode(
            Runnable action,
            com.kafkick.core.support.exception.ErrorCode errorCode
    ) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(errorCode)
                );
    }
}

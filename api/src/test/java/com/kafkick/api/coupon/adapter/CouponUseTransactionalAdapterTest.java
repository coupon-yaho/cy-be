// 별도 멱등 선점·완료 재조회·stale 회수와 실제 JSON 응답 재생을 검증합니다.
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
import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.CouponUseCommand;
import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponUseTransactionalAdapterTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant USED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IdempotencyClaimTransactionalAdapter claimAdapter;

    @Mock
    private CouponUseTransactionExecutor transactionExecutor;

    @Mock
    private TimeProvider timeProvider;

    private CouponUseResponseCodec responseCodec;
    private CouponUseTransactionalAdapter adapter;

    @BeforeEach
    void setUp() {
        responseCodec = new CouponUseResponseCodec(new ObjectMapper());
        adapter = new CouponUseTransactionalAdapter(
                claimAdapter,
                transactionExecutor,
                responseCodec,
                timeProvider,
                new CouponIdempotencyProperties(
                        Duration.ofMillis(100),
                        Duration.ofMillis(1),
                        Duration.ofSeconds(30)
                )
        );
    }

    @Test
    @DisplayName("최초 요청은 동일한 시각으로 선점하고 쿠폰 사용을 실행한다")
    void processFirstRequestWithOneTimestamp() {
        when(timeProvider.instant()).thenReturn(USED_AT);
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenReturn(true);
        CouponUseResponse expected = response();
        when(transactionExecutor.execute(any(), eq(USED_AT)))
                .thenReturn(expected);

        CouponUseResponse actual = adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                new CouponUseRequest(30L, 20_000)
        );

        assertThat(actual).isEqualTo(expected);
        ArgumentCaptor<CouponUseCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponUseCommand.class);
        verify(transactionExecutor).execute(
                commandCaptor.capture(),
                eq(USED_AT)
        );
        assertThat(commandCaptor.getValue().issuanceId()).isEqualTo(100L);
        assertThat(commandCaptor.getValue().memberId()).isEqualTo(20L);
        assertThat(commandCaptor.getValue().orderId()).isEqualTo(30L);
        assertThat(commandCaptor.getValue().orderAmount()).isEqualTo(20_000);
        assertThat(commandCaptor.getValue().idempotencyKey())
                .isEqualTo(IDEMPOTENCY_KEY);
        assertThat(commandCaptor.getValue().usedAt()).isEqualTo(USED_AT);
        verify(timeProvider).instant();
    }

    @Test
    @DisplayName("DONE 요청은 실제 JSON으로 저장된 최초 응답을 복원한다")
    void replayCompletedRequestWithRealObjectMapper() {
        when(timeProvider.instant()).thenReturn(USED_AT);
        CouponUseResponse expected = response();
        String storedResponse = responseCodec.write(expected);
        AtomicReference<String> requestHash = new AtomicReference<>();
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenAnswer(invocation -> {
            requestHash.set(invocation.getArgument(1));
            return false;
        });
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenAnswer(invocation -> Optional.of(record(
                        requestHash.get(),
                        IdempotencyStatus.DONE,
                        storedResponse,
                        USED_AT
                )));

        CouponUseResponse actual = adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                new CouponUseRequest(30L, 20_000)
        );

        assertThat(actual).isEqualTo(expected);
        verify(transactionExecutor, never()).execute(any(), any());
    }

    @Test
    @DisplayName("IN_PROGRESS 요청은 제한 재조회 중 DONE이 되면 최초 응답을 반환한다")
    void waitForInProgressRequest() {
        when(timeProvider.instant()).thenReturn(USED_AT);
        String storedResponse = responseCodec.write(response());
        AtomicReference<String> requestHash = new AtomicReference<>();
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenAnswer(invocation -> {
            requestHash.set(invocation.getArgument(1));
            return false;
        });
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenAnswer(invocation -> Optional.of(record(
                        requestHash.get(),
                        IdempotencyStatus.IN_PROGRESS,
                        null,
                        USED_AT
                )))
                .thenAnswer(invocation -> Optional.of(record(
                        requestHash.get(),
                        IdempotencyStatus.DONE,
                        storedResponse,
                        USED_AT
                )));

        CouponUseResponse actual = adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                new CouponUseRequest(30L, 20_000)
        );

        assertThat(actual).isEqualTo(response());
        verify(claimAdapter, times(2)).findByKey(IDEMPOTENCY_KEY);
        verify(transactionExecutor, never()).execute(any(), any());
    }

    @Test
    @DisplayName("오래된 IN_PROGRESS 요청을 조건부 회수해 다시 처리한다")
    void reclaimStaleInProgressRequest() {
        when(timeProvider.instant()).thenReturn(USED_AT);
        Instant oldClaimedAt = USED_AT.minusSeconds(31);
        AtomicReference<String> requestHash = new AtomicReference<>();
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
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
                eq(USED_AT)
        )).thenReturn(true);
        when(transactionExecutor.execute(any(), eq(USED_AT)))
                .thenReturn(response());

        CouponUseResponse actual = adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                new CouponUseRequest(30L, 20_000)
        );

        assertThat(actual).isEqualTo(response());
        verify(transactionExecutor).execute(any(), eq(USED_AT));
    }

    @Test
    @DisplayName("쿠폰 사용 실패 시 자신이 선점한 IN_PROGRESS 레코드를 해제한다")
    void releaseClaimAfterProcessingFailure() {
        when(timeProvider.instant()).thenReturn(USED_AT);
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenReturn(true);
        BusinessException failure = new BusinessException(
                CouponUseErrorCode.COUPON_EXPIRED
        );
        when(transactionExecutor.execute(any(), eq(USED_AT)))
                .thenThrow(failure);

        assertThatThrownBy(() -> adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                new CouponUseRequest(30L, 20_000)
        )).isSameAs(failure);
        verify(claimAdapter).release(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        );
    }

    @Test
    @DisplayName("같은 멱등키를 다른 요청에 재사용하면 422로 거부한다")
    void rejectReusedKeyForDifferentRequest() {
        when(timeProvider.instant()).thenReturn(USED_AT);
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(USED_AT)
        )).thenReturn(false);
        when(claimAdapter.findByKey(IDEMPOTENCY_KEY))
                .thenReturn(Optional.of(new IdempotencyRecord(
                        IDEMPOTENCY_KEY,
                        20L,
                        100L,
                        "0".repeat(64),
                        IdempotencyStatus.DONE,
                        responseCodec.write(response()),
                        USED_AT
                )));

        assertThatThrownBy(() -> adapter.use(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                new CouponUseRequest(30L, 20_000)
        )).isInstanceOfSatisfying(
                BusinessException.class,
                exception -> assertThat(exception.getErrorCode())
                        .isEqualTo(CouponUseErrorCode.IDEMPOTENCY_KEY_REUSED)
        );
        verify(transactionExecutor, never()).execute(any(), any());
    }

    @Test
    @DisplayName("UUID v4가 아닌 멱등키는 선점 전에 거부한다")
    void rejectInvalidIdempotencyKey() {
        assertThatThrownBy(() -> adapter.use(
                100L,
                20L,
                "not-a-uuid",
                new CouponUseRequest(30L, 20_000)
        )).isInstanceOfSatisfying(
                BusinessException.class,
                exception -> assertThat(exception.getErrorCode())
                        .isEqualTo(
                                CouponUseErrorCode.INVALID_COUPON_USE_REQUEST
                        )
        );
        verify(claimAdapter, never()).tryStart(anyString(), anyString(), any());
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

    private CouponUseResponse response() {
        return new CouponUseResponse(
                100L,
                IssuanceStatus.USED,
                30L,
                5_000,
                USED_AT
        );
    }
}

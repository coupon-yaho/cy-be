// 쿠폰 발급 취소의 멱등 선점·실행·응답 재생을 검증합니다.
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
import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.core.coupon.domain.IdempotencyRecord;
import com.kafkick.core.coupon.domain.IdempotencyStatus;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.service.CouponCancelCommand;
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
class CouponCancelTransactionalAdapterTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440002";
    private static final Instant CANCELED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IdempotencyClaimTransactionalAdapter claimAdapter;

    @Mock
    private CouponCancelTransactionExecutor transactionExecutor;

    @Mock
    private TimeProvider timeProvider;

    private CouponCancelResponseCodec responseCodec;
    private CouponCancelTransactionalAdapter adapter;

    @BeforeEach
    void setUp() {
        responseCodec = new CouponCancelResponseCodec(new ObjectMapper());
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
        adapter = new CouponCancelTransactionalAdapter(
                idempotencyTemplate,
                transactionExecutor,
                responseCodec
        );
    }

    @Test
    @DisplayName("최초 요청은 한 번 생성한 시각으로 선점하고 발급 취소를 실행한다")
    void processFirstRequestWithOneTimestamp() {
        when(timeProvider.instant()).thenReturn(CANCELED_AT);
        when(claimAdapter.tryStart(
                eq(IDEMPOTENCY_KEY),
                anyString(),
                eq(CANCELED_AT)
        )).thenReturn(true);
        when(transactionExecutor.execute(any(), eq(CANCELED_AT)))
                .thenReturn(response());

        CouponCancelResponse actual = adapter.cancel(
                100L,
                20L,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(response());
        ArgumentCaptor<CouponCancelCommand> commandCaptor =
                ArgumentCaptor.forClass(CouponCancelCommand.class);
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
    }

    @Test
    @DisplayName("DONE 요청은 상태와 재고를 다시 변경하지 않고 최초 발급 취소 응답을 반환한다")
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
                .thenAnswer(invocation -> Optional.of(new IdempotencyRecord(
                        IDEMPOTENCY_KEY,
                        20L,
                        100L,
                        requestHash.get(),
                        IdempotencyStatus.DONE,
                        responseCodec.write(response()),
                        CANCELED_AT
                )));

        CouponCancelResponse actual = adapter.cancel(
                100L,
                20L,
                IDEMPOTENCY_KEY
        );

        assertThat(actual).isEqualTo(response());
        verify(transactionExecutor, never()).execute(any(), any());
    }

    @Test
    @DisplayName("UUID v4가 아닌 멱등키는 발급 취소 선점 전에 거부한다")
    void rejectInvalidIdempotencyKey() {
        assertThatThrownBy(() -> adapter.cancel(100L, 20L, "invalid-key"))
                .isInstanceOfSatisfying(
                        BusinessException.class,
                        exception -> assertThat(exception.getErrorCode())
                                .isEqualTo(CouponUseErrorCode
                                        .INVALID_COUPON_CANCEL_REQUEST)
                );
        verify(claimAdapter, never()).tryStart(any(), any(), any());
        verify(timeProvider, never()).instant();
    }

    private CouponCancelResponse response() {
        return new CouponCancelResponse(
                100L,
                IssuanceStatus.CANCELLED,
                CANCELED_AT
        );
    }
}

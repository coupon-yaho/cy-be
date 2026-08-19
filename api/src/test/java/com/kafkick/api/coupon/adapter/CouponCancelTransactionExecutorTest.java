// 발급 취소 결과와 멱등 완료 응답이 같은 트랜잭션 경계에서 저장되는지 검증합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.service.CouponCancelCommand;
import com.kafkick.core.coupon.service.CouponCancelResult;
import com.kafkick.core.coupon.service.CouponCancelService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponCancelTransactionExecutorTest {

    private static final String IDEMPOTENCY_KEY =
            "550e8400-e29b-41d4-a716-446655440002";
    private static final Instant CANCELED_AT =
            Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private CouponCancelService cancelService;

    @Mock
    private IdempotencyRepository idempotencyRepository;

    @Test
    @DisplayName("발급 취소 결과를 반환하고 동일 응답으로 멱등 레코드를 완료한다")
    void completeIdempotencyWithCancelResponse() {
        CouponCancelResponseCodec responseCodec =
                new CouponCancelResponseCodec(new ObjectMapper());
        CouponCancelTransactionExecutor executor =
                new CouponCancelTransactionExecutor(
                        cancelService,
                        idempotencyRepository,
                        responseCodec
                );
        CouponCancelCommand command = new CouponCancelCommand(
                100L,
                20L,
                IDEMPOTENCY_KEY,
                CANCELED_AT
        );
        when(cancelService.cancel(command)).thenReturn(new CouponCancelResult(
                100L,
                IssuanceStatus.CANCELLED,
                CANCELED_AT
        ));

        CouponCancelResponse response = executor.execute(
                command,
                CANCELED_AT
        );

        assertThat(response).isEqualTo(new CouponCancelResponse(
                100L,
                IssuanceStatus.CANCELLED,
                CANCELED_AT
        ));
        ArgumentCaptor<String> responseBody =
                ArgumentCaptor.forClass(String.class);
        verify(idempotencyRepository).complete(
                eq(IDEMPOTENCY_KEY),
                eq(20L),
                eq(100L),
                responseBody.capture(),
                eq(CANCELED_AT)
        );
        assertThat(responseCodec.read(responseBody.getValue()))
                .isEqualTo(response);
    }
}

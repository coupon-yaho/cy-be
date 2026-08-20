package com.kafkick.core.coupon.service;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponOperationExecutionServiceTest {

    private static final String KEY =
            "550e8400-e29b-41d4-a716-446655440000";
    private static final Instant AT = Instant.parse("2026-08-20T05:00:00Z");

    @Mock
    private IdempotencyExecutionService idempotencyExecutionService;
    @Mock
    private IdempotentOperationService operationService;
    @Mock
    private CouponUseService couponUseService;
    @Mock
    private CouponCancelUseService couponCancelUseService;
    @Mock
    private CouponCancelService couponCancelService;
    @Mock
    private IdempotencyResultCodec<CouponUseResult> useCodec;
    @Mock
    private IdempotencyResultCodec<CouponCancelUseResult> cancelUseCodec;
    @Mock
    private IdempotencyResultCodec<CouponCancelResult> cancelCodec;

    @Test
    void executesCouponUseThroughCoreIdempotencyOrchestration() {
        CouponUseResult expected = new CouponUseResult(
                100L, IssuanceStatus.USED, 30L, 5_000, AT
        );
        when(idempotencyExecutionService.execute(
                eq(KEY), any(), any(), any(), any()
        )).thenAnswer(invocation -> {
            java.util.function.Function<Instant, CouponUseResult> claimed =
                    invocation.getArgument(3);
            return claimed.apply(AT);
        });
        when(operationService.execute(
                eq(KEY), eq(20L), eq(100L), eq(AT), any(), eq(useCodec)
        )).thenAnswer(invocation -> {
            java.util.function.Supplier<CouponUseResult> operation =
                    invocation.getArgument(4);
            return operation.get();
        });
        when(couponUseService.use(any())).thenReturn(expected);
        CouponOperationExecutionService service = service();

        CouponUseResult actual = service.use(
                100L, 20L, 30L, 20_000, KEY
        );

        assertThat(actual).isEqualTo(expected);
    }

    private CouponOperationExecutionService service() {
        return new CouponOperationExecutionService(
                idempotencyExecutionService,
                operationService,
                couponUseService,
                couponCancelUseService,
                couponCancelService,
                useCodec,
                cancelUseCodec,
                cancelCodec
        );
    }
}

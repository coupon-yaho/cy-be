// 쿠폰 API가 공유하는 멱등 선점·실행 흐름과 요청별 전략 주입을 검증합니다.
package com.kafkick.api.coupon.adapter;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.api.coupon.config.CouponIdempotencyProperties;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.support.TimeProvider;

import static org.assertj.core.api.Assertions.assertThat;
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
}

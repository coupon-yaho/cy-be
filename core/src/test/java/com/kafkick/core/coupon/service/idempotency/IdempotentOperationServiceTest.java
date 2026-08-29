package com.kafkick.core.coupon.service.idempotency;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.port.IdempotencyRepository;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotentOperationServiceTest {

    @Mock
    private IdempotencyRepository idempotencyRepository;

    @Mock
    private IdempotencyResultCodec<String> resultCodec;

    @Test
    void completesClaimInSameOperationBoundary() {
        Instant claimedAt = Instant.parse("2026-08-20T05:00:00Z");
        when(resultCodec.write("result")).thenReturn("stored-result");
        IdempotentOperationService service = new IdempotentOperationService(
                idempotencyRepository
        );

        String result = service.execute(
                "550e8400-e29b-41d4-a716-446655440000",
                20L,
                claimedAt,
                () -> "result",
                resultCodec,
                ignored -> 100L
        );

        assertThat(result).isEqualTo("result");
        verify(idempotencyRepository).complete(
                "550e8400-e29b-41d4-a716-446655440000",
                20L,
                100L,
                "stored-result",
                claimedAt
        );
    }
}

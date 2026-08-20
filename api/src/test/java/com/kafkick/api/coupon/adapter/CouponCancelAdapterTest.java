package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.CouponCancelResult;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponCancelAdapterTest {

    @Mock
    private CouponOperationExecutionService executionService;

    @Test
    void mapsCoreCancelResultToHttpResponse() {
        Instant canceledAt = Instant.parse("2026-08-20T05:00:00Z");
        when(executionService.cancel(100L, 20L, "key"))
                .thenReturn(new CouponCancelResult(
                        100L, IssuanceStatus.CANCELLED, canceledAt
                ));
        CouponCancelAdapter adapter =
                new CouponCancelAdapter(executionService);

        var response = adapter.cancel(100L, 20L, "key");

        assertThat(response.issuanceId()).isEqualTo(100L);
        assertThat(response.status()).isEqualTo(IssuanceStatus.CANCELLED);
        assertThat(response.canceledAt()).isEqualTo(canceledAt);
    }
}

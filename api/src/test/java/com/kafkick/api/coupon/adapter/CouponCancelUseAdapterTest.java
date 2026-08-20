package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.CouponCancelUseResult;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponCancelUseAdapterTest {

    @Mock
    private CouponOperationExecutionService executionService;

    @Test
    void mapsCoreCancelUseResultToHttpResponse() {
        Instant canceledAt = Instant.parse("2026-08-20T05:00:00Z");
        when(executionService.cancelUse(100L, 20L, "key"))
                .thenReturn(new CouponCancelUseResult(
                        100L, IssuanceStatus.ISSUED, 30L, 5_000, canceledAt
                ));
        CouponCancelUseAdapter adapter =
                new CouponCancelUseAdapter(executionService);

        var response = adapter.cancelUse(100L, 20L, "key");

        assertThat(response.issuanceId()).isEqualTo(100L);
        assertThat(response.status()).isEqualTo(IssuanceStatus.ISSUED);
        assertThat(response.canceledAt()).isEqualTo(canceledAt);
    }
}

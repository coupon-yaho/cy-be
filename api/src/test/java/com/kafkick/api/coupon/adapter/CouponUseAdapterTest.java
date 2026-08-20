package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.kafkick.api.coupon.dto.CouponUseRequest;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.CouponOperationExecutionService;
import com.kafkick.core.coupon.service.CouponUseResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouponUseAdapterTest {

    @Mock
    private CouponOperationExecutionService executionService;

    @Test
    void mapsCoreUseResultToHttpResponse() {
        Instant usedAt = Instant.parse("2026-08-20T05:00:00Z");
        when(executionService.use(100L, 20L, 30L, 20_000, "key"))
                .thenReturn(new CouponUseResult(
                        100L, IssuanceStatus.USED, 30L, 5_000, usedAt
                ));
        CouponUseAdapter adapter =
                new CouponUseAdapter(executionService);

        var response = adapter.use(
                100L, 20L, "key", new CouponUseRequest(30L, 20_000)
        );

        assertThat(response.issuanceId()).isEqualTo(100L);
        assertThat(response.status()).isEqualTo(IssuanceStatus.USED);
        assertThat(response.usedAt()).isEqualTo(usedAt);
    }
}

package com.kafkick.api.coupon.adapter;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.service.CouponCancelResult;
import com.kafkick.core.coupon.service.CouponCancelUseResult;
import com.kafkick.core.coupon.service.CouponUseResult;

import static org.assertj.core.api.Assertions.assertThat;

class CouponResponseCodecTest {

    private static final Instant AT = Instant.parse("2026-08-20T05:00:00Z");

    @Test
    void roundTripsCoreOperationResults() {
        ObjectMapper objectMapper = new ObjectMapper();
        CouponUseResponseCodec useCodec = new CouponUseResponseCodec(
                objectMapper
        );
        CouponCancelUseResponseCodec cancelUseCodec =
                new CouponCancelUseResponseCodec(objectMapper);
        CouponCancelResponseCodec cancelCodec = new CouponCancelResponseCodec(
                objectMapper
        );
        CouponUseResult use = new CouponUseResult(
                1L, IssuanceStatus.USED, 10L, 5_000, AT
        );
        CouponCancelUseResult cancelUse = new CouponCancelUseResult(
                1L, IssuanceStatus.ISSUED, 10L, 5_000, AT
        );
        CouponCancelResult cancel = new CouponCancelResult(
                1L, IssuanceStatus.CANCELLED, AT
        );

        assertThat(useCodec.read(useCodec.write(use))).isEqualTo(use);
        assertThat(cancelUseCodec.read(cancelUseCodec.write(cancelUse)))
                .isEqualTo(cancelUse);
        assertThat(cancelCodec.read(cancelCodec.write(cancel)))
                .isEqualTo(cancel);
    }
}

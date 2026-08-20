package com.kafkick.api.coupon.adapter;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;
import tools.jackson.databind.ObjectMapper;

import com.kafkick.api.coupon.exception.IdempotencyResponseCodecException;
import com.kafkick.core.coupon.domain.IssuanceStatus;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.CouponCancelResult;
import com.kafkick.core.coupon.service.CouponCancelUseResult;
import com.kafkick.core.coupon.service.CouponUseResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@JsonTest
class CouponResponseCodecTest {

    private static final Instant AT = Instant.parse("2026-08-20T05:00:00Z");

    private final ObjectMapper objectMapper;

    @Autowired
    CouponResponseCodecTest(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Test
    void roundTripsCoreOperationResults() {
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

    @Test
    void mapsAllCodecFailuresToIdempotencyErrorCode() {
        List<IdempotencyResultCodec<?>> codecs = List.of(
                new CouponUseResponseCodec(objectMapper),
                new CouponCancelUseResponseCodec(objectMapper),
                new CouponCancelResponseCodec(objectMapper)
        );

        for (IdempotencyResultCodec<?> codec : codecs) {
            assertThatThrownBy(() -> codec.read("{"))
                    .isInstanceOfSatisfying(
                            IdempotencyResponseCodecException.class,
                            exception -> assertThat(exception)
                                    .extracting("errorCode")
                                    .isEqualTo(
                                            CouponUseErrorCode
                                                    .IDEMPOTENCY_SAVE_FAILED
                                    )
                    );
        }
    }
}

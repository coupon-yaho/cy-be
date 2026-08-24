package com.kafkick.storage.db.coupon.codec;

import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.result.CouponUseResult;

@Component
public class CouponUseResultCodec
        extends JacksonIdempotencyResultCodec<CouponUseResult> {

    public CouponUseResultCodec(ObjectMapper objectMapper) {
        super(objectMapper, CouponUseResult.class, "쿠폰 사용");
    }
}

package com.kafkick.storage.db.coupon.codec;

import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.result.CouponCancelResult;

@Component
public class CouponCancelResultCodec
        extends JacksonIdempotencyResultCodec<CouponCancelResult> {

    public CouponCancelResultCodec(ObjectMapper objectMapper) {
        super(objectMapper, CouponCancelResult.class, "쿠폰 발급 취소");
    }
}

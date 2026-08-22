package com.kafkick.storage.db.coupon.codec;

import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.result.CouponCancelUseResult;

@Component
public class CouponCancelUseResultCodec
        extends JacksonIdempotencyResultCodec<CouponCancelUseResult> {

    public CouponCancelUseResultCodec(ObjectMapper objectMapper) {
        super(objectMapper, CouponCancelUseResult.class, "쿠폰 사용 취소");
    }
}

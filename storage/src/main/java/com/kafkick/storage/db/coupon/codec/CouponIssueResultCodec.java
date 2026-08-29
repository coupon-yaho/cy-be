package com.kafkick.storage.db.coupon.codec;

import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.service.result.CouponIssueResult;

@Component
public class CouponIssueResultCodec
        extends JacksonIdempotencyResultCodec<CouponIssueResult> {

    public CouponIssueResultCodec(ObjectMapper objectMapper) {
        super(objectMapper, CouponIssueResult.class, "쿠폰 발급");
    }
}

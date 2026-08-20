package com.kafkick.api.coupon.adapter;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.exception.IdempotencyResponseCodecException;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.CouponUseResult;

@Component
public class CouponUseResponseCodec
        implements IdempotencyResultCodec<CouponUseResult> {

    private final ObjectMapper objectMapper;

    public CouponUseResponseCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String write(CouponUseResult response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JacksonException exception) {
            throw new IdempotencyResponseCodecException(
                    "쿠폰 사용 응답 직렬화에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public CouponUseResult read(String responseBody) {
        try {
            return objectMapper.readValue(
                    responseBody,
                    CouponUseResult.class
            );
        } catch (JacksonException exception) {
            throw new IdempotencyResponseCodecException(
                    "저장된 쿠폰 사용 응답을 읽지 못했습니다.",
                    exception
            );
        }
    }
}

package com.kafkick.api.coupon.adapter;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.exception.IdempotencyResponseCodecException;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.CouponCancelUseResult;

@Component
public class CouponCancelUseResponseCodec
        implements IdempotencyResultCodec<CouponCancelUseResult> {

    private final ObjectMapper objectMapper;

    public CouponCancelUseResponseCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String write(CouponCancelUseResult response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JacksonException exception) {
            throw new IdempotencyResponseCodecException(
                    "쿠폰 사용 취소 응답 직렬화에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public CouponCancelUseResult read(String responseBody) {
        try {
            return objectMapper.readValue(
                    responseBody,
                    CouponCancelUseResult.class
            );
        } catch (JacksonException exception) {
            throw new IdempotencyResponseCodecException(
                    "저장된 쿠폰 사용 취소 응답을 읽지 못했습니다.",
                    exception
            );
        }
    }
}

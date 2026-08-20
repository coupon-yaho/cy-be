package com.kafkick.api.coupon.adapter;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.exception.IdempotencyResponseCodecException;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.CouponCancelResult;

@Component
public class CouponCancelResponseCodec
        implements IdempotencyResultCodec<CouponCancelResult> {

    private final ObjectMapper objectMapper;

    public CouponCancelResponseCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public String write(CouponCancelResult response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JacksonException exception) {
            throw new IdempotencyResponseCodecException(
                    "쿠폰 발급 취소 응답 직렬화에 실패했습니다.",
                    exception
            );
        }
    }

    @Override
    public CouponCancelResult read(String responseBody) {
        try {
            return objectMapper.readValue(
                    responseBody,
                    CouponCancelResult.class
            );
        } catch (JacksonException exception) {
            throw new IdempotencyResponseCodecException(
                    "저장된 쿠폰 발급 취소 응답을 읽지 못했습니다.",
                    exception
            );
        }
    }
}

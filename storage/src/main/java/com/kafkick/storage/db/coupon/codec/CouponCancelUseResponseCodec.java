package com.kafkick.storage.db.coupon.codec;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.core.coupon.exception.IdempotencyPersistenceException;
import com.kafkick.core.coupon.port.IdempotencyResultCodec;
import com.kafkick.core.coupon.service.result.CouponCancelUseResult;

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
            throw new IdempotencyPersistenceException(
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
            throw new IdempotencyPersistenceException(
                    "저장된 쿠폰 사용 취소 응답을 읽지 못했습니다.",
                    exception
            );
        }
    }
}

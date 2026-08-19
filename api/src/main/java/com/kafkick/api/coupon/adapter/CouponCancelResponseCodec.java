// 멱등한 발급 취소 응답을 실제 API ObjectMapper로 저장하고 복원합니다.
package com.kafkick.api.coupon.adapter;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponCancelResponse;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.support.exception.BusinessException;

@Component
public class CouponCancelResponseCodec {

    private final ObjectMapper objectMapper;

    public CouponCancelResponseCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String write(CouponCancelResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JacksonException exception) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED,
                    "쿠폰 발급 취소 응답 직렬화에 실패했습니다.",
                    exception
            );
        }
    }

    public CouponCancelResponse read(String responseBody) {
        try {
            return objectMapper.readValue(
                    responseBody,
                    CouponCancelResponse.class
            );
        } catch (JacksonException exception) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED,
                    "저장된 쿠폰 발급 취소 응답을 읽지 못했습니다.",
                    exception
            );
        }
    }
}

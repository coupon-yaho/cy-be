// 멱등 응답을 실제 API ObjectMapper로 직렬화하고 동일한 응답으로 복원합니다.
package com.kafkick.api.coupon.adapter;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.kafkick.api.coupon.dto.CouponUseResponse;
import com.kafkick.core.coupon.exception.CouponUseErrorCode;
import com.kafkick.core.support.exception.BusinessException;

@Component
public class CouponUseResponseCodec {

    private final ObjectMapper objectMapper;

    public CouponUseResponseCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String write(CouponUseResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JacksonException exception) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED,
                    "쿠폰 사용 응답 직렬화에 실패했습니다.",
                    exception
            );
        }
    }

    public CouponUseResponse read(String responseBody) {
        try {
            return objectMapper.readValue(
                    responseBody,
                    CouponUseResponse.class
            );
        } catch (JacksonException exception) {
            throw new BusinessException(
                    CouponUseErrorCode.IDEMPOTENCY_SAVE_FAILED,
                    "저장된 쿠폰 사용 응답을 읽지 못했습니다.",
                    exception
            );
        }
    }
}

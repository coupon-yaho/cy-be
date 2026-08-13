// 쿠폰 생성 과정에서 발생한 입력 및 도메인 검증 오류를 400 응답으로 변환합니다.
package com.kafkick.api.coupon.exception;

import com.kafkick.api.coupon.controller.CouponTemplateController;
import com.kafkick.api.coupon.dto.CouponErrorResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice(assignableTypes = CouponTemplateController.class)
public class CouponExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public CouponErrorResponse handleDomainValidation(
            IllegalArgumentException exception
    ) {
        return CouponErrorResponse.of(
                "INVALID_COUPON",
                exception.getMessage()
        );
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public CouponErrorResponse handleRequestValidation(
            MethodArgumentNotValidException exception
    ) {
        Map<String, String> fieldErrors = new LinkedHashMap<>();

        for (FieldError fieldError
                : exception.getBindingResult().getFieldErrors()) {
            fieldErrors.putIfAbsent(
                    fieldError.getField(),
                    fieldError.getDefaultMessage()
            );
        }

        return CouponErrorResponse.of(
                "INVALID_REQUEST",
                "요청값을 확인해 주세요.",
                fieldErrors
        );
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public CouponErrorResponse handleUnreadableRequest() {
        return CouponErrorResponse.of(
                "INVALID_REQUEST",
                "요청 본문의 형식이나 Enum 값을 확인해 주세요."
        );
    }
}

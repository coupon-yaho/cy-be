package com.kafkick.api.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import jakarta.validation.ConstraintViolationException;

import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.response.ErrorResponse;
import com.kafkick.core.support.response.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;
import com.kafkick.core.support.exception.ErrorCode;

/**
 * 모든 에러를 성공 응답과 같은 봉투로 감싼다. HTTP status 는 실제 4xx/5xx 를 유지한다.
 * 응답에는 errorCode 카탈로그 메시지만 담고, 예외 detail 과 스택은 로그에만 남긴다.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private static final String REQUEST_ID = "requestId";
    private static final String VALIDATION_FALLBACK_MESSAGE = "잘못된 요청입니다.";

    private final TimeProvider timeProvider;

    public GlobalExceptionHandler(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
    }

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleBusinessException(BusinessException exception) {
        ErrorCode errorCode = exception.getErrorCode();
        if (errorCode.getStatus() >= 500) {
            log.error("[{}] {}", errorCode.getCode(), exception.getMessage(), exception);
        } else {
            // 재고 소진처럼 정상 흐름에서 대량 발생하므로 스택은 남기지 않는다.
            log.warn("[{}] {}", errorCode.getCode(), exception.getMessage());
        }
        return ResponseEntity.status(errorCode.getStatus())
                .body(ResponseEnvelope.fail(body(errorCode)));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleConstraintViolation(ConstraintViolationException exception) {
        String message = exception.getConstraintViolations().stream()
                .findFirst()
                .map(violation -> violation.getMessage())
                .orElse(VALIDATION_FALLBACK_MESSAGE);
        log.warn("[{}] validation: {}", CommonErrorCode.INVALID_INPUT.getCode(), message);
        return ResponseEntity.badRequest().body(ResponseEnvelope.fail(validationBody(message)));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleUnexpected(Exception exception) {
        log.error("Unhandled exception", exception);
        return ResponseEntity.status(CommonErrorCode.INTERNAL_ERROR.getStatus())
                .body(ResponseEnvelope.fail(body(CommonErrorCode.INTERNAL_ERROR)));
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        String message = ex.getBindingResult().getAllErrors().stream()
                .findFirst()
                .map(MessageSourceResolvable::getDefaultMessage)
                .orElse(VALIDATION_FALLBACK_MESSAGE);
        log.warn("[{}] validation: {}", CommonErrorCode.INVALID_INPUT.getCode(), message);
        return super.handleExceptionInternal(
                ex, ResponseEnvelope.fail(validationBody(message)), headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleHandlerMethodValidationException(
            HandlerMethodValidationException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        String message = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream())
                .findFirst()
                .map(MessageSourceResolvable::getDefaultMessage)
                .orElse(VALIDATION_FALLBACK_MESSAGE);
        log.warn("[{}] validation: {}", CommonErrorCode.INVALID_INPUT.getCode(), message);
        return super.handleExceptionInternal(
                ex, ResponseEnvelope.fail(validationBody(message)), headers, status, request);
    }

    /** 표준 MVC 예외(404·405·415·깨진 JSON 등)가 모두 지나는 공통 지점. */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(
            Exception ex, Object body, HttpHeaders headers, HttpStatusCode statusCode, WebRequest request) {
        // 위 검증 핸들러들이 이미 봉투로 감싸 호출하므로 이중 포장을 피한다.
        Object envelope = (body instanceof ResponseEnvelope<?>)
                ? body
                : wrapStandardError(ex, statusCode);
        return super.handleExceptionInternal(ex, envelope, headers, statusCode, request);
    }

    private ResponseEnvelope<Void> wrapStandardError(Exception ex, HttpStatusCode statusCode) {
        ErrorResponse error = standardErrorBody(statusCode);
        log.warn("[{}] {} {}", error.code(), statusCode.value(), ex.getClass().getSimpleName());
        return ResponseEnvelope.fail(error);
    }

    private ErrorResponse standardErrorBody(HttpStatusCode statusCode) {
        CommonErrorCode mapped = switch (statusCode.value()) {
            case 404 -> CommonErrorCode.NOT_FOUND;
            case 405 -> CommonErrorCode.METHOD_NOT_ALLOWED;
            default -> statusCode.is4xxClientError()
                    ? CommonErrorCode.INVALID_INPUT
                    : CommonErrorCode.INTERNAL_ERROR;
        };
        // status 는 매핑된 코드가 아니라 실제 statusCode 를 쓴다. 415·406 등이 400 으로 뭉개지지 않게.
        return new ErrorResponse(
                statusCode.value(), mapped.getCode(), mapped.getMessage(), requestId(), timeProvider.instant());
    }

    private ErrorResponse body(ErrorCode errorCode) {
        return ErrorResponse.of(errorCode, requestId(), timeProvider.instant());
    }

    private ErrorResponse validationBody(String message) {
        ErrorCode errorCode = CommonErrorCode.INVALID_INPUT;
        return new ErrorResponse(
                errorCode.getStatus(), errorCode.getCode(), message, requestId(), timeProvider.instant());
    }

    private String requestId() {
        return MDC.get(REQUEST_ID);
    }
}

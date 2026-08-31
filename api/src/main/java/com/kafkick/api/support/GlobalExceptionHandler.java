package com.kafkick.api.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import jakarta.validation.ConstraintViolationException;
import jakarta.servlet.http.HttpServletRequest;

import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.ServletRequestBindingException;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.runtimeconfig.RuntimeConfigRevisionConflictException;
import com.kafkick.core.support.exception.CommonErrorCode;
import com.kafkick.core.support.exception.ErrorCode;
import com.kafkick.core.observation.Dependency;
import com.kafkick.core.observation.RequestAttributeKeys;
import com.kafkick.api.admin.benchmark.TopologyValidationException;
import com.kafkick.api.support.auth.RequestHeaderContractException;

/**
 * 모든 에러를 성공 응답과 같은 봉투로 감싼다. HTTP status 는 실제 4xx/5xx 를 유지한다.
 *
 * <p><b>응답 메시지가 늘 카탈로그 문구인 것은 아니다.</b> 업무 예외는 {@code errorCode} 의
 * 카탈로그 메시지를 그대로 쓰지만, 검증 실패는 제약이 선언한 문구를, 헤더 누락은 빠진
 * 헤더 이름을 싣는다 — 호출자가 <b>무엇을 고쳐야 하는지</b> 응답만 보고 알아야 하는
 * 자리들이다. 한때 이 문단이 "카탈로그 메시지만 담는다" 고 단정했는데, 그때도 검증
 * 갈래는 이미 제약 문구를 싣고 있었다.
 *
 * <p><b>어디까지 어디로 가는지는 둘로 갈린다.</b> 섞어 적으면 다음 사람이 요청 값을
 * 로그에 남겨도 되는 것으로 읽는다.
 *
 * <ul>
 *   <li><b>예외 detail 과 스택</b> — 응답에는 안 넣고 <b>로그에만</b> 남긴다. 업무 예외의
 *       detail 에는 식별자가 들어 있을 수 있고(예: {@code memberId=…}), 그것이 응답으로
 *       새지 않는지는 {@code GlobalExceptionHandlerTest} 가 지킨다.</li>
 *   <li><b>요청 헤더·파라미터의 값</b> — <b>응답에도 로그에도 안 남긴다.</b> 헤더 누락
 *       갈래가 싣는 것은 헤더 <i>이름</i> 하나뿐이고, 검증 갈래가 싣는 것은 제약이 코드에
 *       선언한 문구다. 둘 다 요청 내용을 되비추지 않는다.</li>
 * </ul>
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
    public ResponseEntity<ResponseEnvelope<Void>> handleBusinessException(
            BusinessException exception, HttpServletRequest request) {
        ErrorCode errorCode = exception.getErrorCode();
        setDependency(request, errorCode.dependency());
        if (errorCode.getStatus() >= 500 && errorCode.logStackTrace()) {
            log.error("[{}] {}", errorCode.getCode(), exception.getMessage(), exception);
        } else if (errorCode.getStatus() >= 500) {
            // 의존성 장애 동안 초당 수천 건이 되는 완화 응답이다. 스택을 찍으면 로그 I/O 가
            // 응답 지연을 밀어 올려 측정 자체가 오염된다.
            log.error("[{}] {}: {}", errorCode.getCode(), exception.getMessage(),
                    exception.getCause() == null ? "-" : exception.getCause().toString());
        } else {
            // 재고 소진처럼 정상 흐름에서 대량 발생하므로 스택은 남기지 않는다.
            log.warn("[{}] {}", errorCode.getCode(), exception.getMessage());
        }
        ResponseEntity.BodyBuilder response = ResponseEntity.status(errorCode.getStatus());
        if (exception instanceof RetryAfterException retryAfter) {
            // 서버가 대신 기다리지 않고 클라이언트가 기다리게 한다.
            response.header(HttpHeaders.RETRY_AFTER,
                    Integer.toString(retryAfter.retryAfterSeconds()));
        }
        return response.body(ResponseEnvelope.fail(body(exception)));
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleConstraintViolation(
            ConstraintViolationException exception, HttpServletRequest request) {
        setDependency(request, Dependency.NONE);
        String message = exception.getConstraintViolations().stream()
                .findFirst()
                .map(violation -> violation.getMessage())
                .orElse(VALIDATION_FALLBACK_MESSAGE);
        log.warn("[{}] validation: {}", CommonErrorCode.INVALID_INPUT.getCode(), message);
        return ResponseEntity.badRequest().body(ResponseEnvelope.fail(validationBody(message)));
    }

    /**
     * <b>헤더 계약 위반은 문구를 응답에 그대로 싣는다.</b>
     *
     * <p>같은 400 이라도 {@code BusinessException} 은 카탈로그 문구만 나가고 detail 은 로그에
     * 남는다 — detail 에 식별자가 들어 있을 수 있어서다. 헤더 계약은 그 반대다. <b>호출자가
     * 무엇을 고쳐야 하는지 응답만 보고 알아야</b> 하고, 문구는 코드가 정한 고정 문자열이라
     * 요청 내용을 되비추지 않는다. 제약 애노테이션 문구를 싣는 것과 같은 등급이다.
     *
     * <p>이것이 없던 동안 등급 헤더 이름이 게이트웨이와 어긋난 요청은 {@code "잘못된
     * 요청입니다."} 만 돌려줬고, 양쪽 담당자가 그 400 을 각자 한참 들여다봤다.
     */
    @ExceptionHandler(RequestHeaderContractException.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleRequestHeaderContract(
            RequestHeaderContractException exception, HttpServletRequest request) {
        setDependency(request, Dependency.NONE);
        log.warn("[{}] header: {}", CommonErrorCode.INVALID_INPUT.getCode(), exception.getMessage());
        return ResponseEntity.badRequest()
                .body(ResponseEnvelope.fail(validationBody(exception.getMessage())));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleUnexpected(
            Exception exception, HttpServletRequest request) {
        setDependency(request, CommonErrorCode.INTERNAL_ERROR.dependency());
        log.error("Unhandled exception", exception);
        return ResponseEntity.status(CommonErrorCode.INTERNAL_ERROR.getStatus())
                .body(ResponseEnvelope.fail(body(CommonErrorCode.INTERNAL_ERROR)));
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        setDependency(request, Dependency.NONE);
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
        setDependency(request, Dependency.NONE);
        String message = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream())
                .findFirst()
                .map(MessageSourceResolvable::getDefaultMessage)
                .orElse(VALIDATION_FALLBACK_MESSAGE);
        log.warn("[{}] validation: {}", CommonErrorCode.INVALID_INPUT.getCode(), message);
        return super.handleExceptionInternal(
                ex, ResponseEnvelope.fail(validationBody(message)), headers, status, request);
    }

    /**
     * <b>어느 헤더가 없는지 응답에 적는다.</b>
     *
     * <p>이 갈래는 원래 공통 지점으로 빠져 {@code "잘못된 요청입니다."} 만 나갔다. 그러면
     * 호출자가 받는 정보는 <b>400 이라는 사실뿐</b>이라, 헤더 이름 하나가 어긋났을 때
     * 서버 결함과 구분이 안 된다. 실제로 대기열 게이트웨이가 {@code X-Member-Grade} 를
     * 보내고 발급이 {@code X-Membership-Grade} 를 요구하던 동안, 양쪽 담당자가 이 400 을
     * 각자 한참 들여다봤다. 이름을 맞추는 것으로는 <b>다음번 다른 헤더</b>를 못 막는다.
     *
     * <p><b>헤더 <i>이름</i>만 싣는다.</b> 이름은 API 계약이라 이미 공개돼 있고, 값은 회원
     * 식별자나 등급이라 응답에도 로그에도 넣지 않는다 — 위 클래스 주석이 적은 경계
     * ("요청에서 온 값은 안 되비춘다") 를 그대로 따른다.
     *
     * <p>헤더 누락이 아닌 다른 바인딩 실패(경로 변수·요청 파라미터 등)는 이름을 특정할 수
     * 없으므로 기존 문구를 그대로 쓴다.
     */
    @Override
    protected ResponseEntity<Object> handleServletRequestBindingException(
            ServletRequestBindingException ex, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        setDependency(request, Dependency.NONE);
        String message = (ex instanceof MissingRequestHeaderException missing)
                ? "필수 요청 헤더가 없습니다: " + missing.getHeaderName()
                : VALIDATION_FALLBACK_MESSAGE;
        log.warn("[{}] binding: {}", CommonErrorCode.INVALID_INPUT.getCode(), message);
        return super.handleExceptionInternal(
                ex, ResponseEnvelope.fail(validationBody(message)), headers, status, request);
    }

    /** 표준 MVC 예외(404·405·415·깨진 JSON 등)가 모두 지나는 공통 지점. */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(
            Exception ex, Object body, HttpHeaders headers, HttpStatusCode statusCode, WebRequest request) {
        // 표준 MVC 예외는 인프라 ErrorCode를 운반하지 않는다. 5xx도 애플리케이션 실패로 고정한다.
        setDependency(request, Dependency.NONE);
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

    private static void setDependency(HttpServletRequest request, Dependency dependency) {
        request.setAttribute(RequestAttributeKeys.DEPENDENCY, dependency);
    }

    private static void setDependency(WebRequest request, Dependency dependency) {
        if (request instanceof ServletWebRequest servletWebRequest) {
            setDependency(servletWebRequest.getRequest(), dependency);
        }
        // 비-Servlet 호출에서는 관측 속성을 생략하되 원래 에러 응답은 계속 만든다.
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
                statusCode.value(), mapped.getCode(), mapped.getMessage(), null,
                null,
                requestId(), timeProvider.instant());
    }

    private ErrorResponse body(ErrorCode errorCode) {
        return ErrorResponse.of(errorCode, requestId(), timeProvider.instant());
    }

    private ErrorResponse body(BusinessException exception) {
        if (exception instanceof RuntimeConfigRevisionConflictException conflict) {
            ErrorCode errorCode = conflict.getErrorCode();
            return new ErrorResponse(
                    errorCode.getStatus(), errorCode.getCode(), errorCode.getMessage(),
                    conflict.getCurrentRevision(), null, requestId(), timeProvider.instant());
        }
        if (exception instanceof TopologyValidationException topology) {
            ErrorCode errorCode = topology.getErrorCode();
            return new ErrorResponse(
                errorCode.getStatus(), errorCode.getCode(), errorCode.getMessage(), null,
                topology.violations(), requestId(), timeProvider.instant());
        }
        return body(exception.getErrorCode());
    }

    private ErrorResponse validationBody(String message) {
        ErrorCode errorCode = CommonErrorCode.INVALID_INPUT;
        return new ErrorResponse(
                errorCode.getStatus(), errorCode.getCode(), message, null,
                null,
                requestId(), timeProvider.instant());
    }

    private String requestId() {
        return MDC.get(REQUEST_ID);
    }
}

// batch 업무 포트의 에러를 도메인 에러코드로 옮깁니다.
package com.kafkick.batch.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.response.ErrorResponse;
import com.kafkick.core.support.response.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;
import com.kafkick.core.support.exception.ErrorCode;

/**
 * <b>이게 없으면 모든 도메인 예외가 500 으로 나간다.</b> {@code BusinessException} 이
 * {@code ErrorCode.getStatus()} 를 들고 있어도, 그것을 읽어 HTTP 로 옮기는 것이 없으면
 * 스프링은 그냥 서버 오류로 접는다 — <i>"스케줄러가 켜져 있어서 거절"</i>(409)과
 * <i>"스키마가 없음"</i>(500)이 클라이언트에게 같은 것으로 보인다.
 *
 * <p><b>{@code api} 모듈의 {@code GlobalExceptionHandler} 를 쓸 수 없다.</b> batch 는
 * {@code core} 만 의존하고 {@code api} 는 안 본다 — 반대로 의존을 걸면 배치가 웹 계층을
 * 끌고 온다. 그래서 <b>같은 규약을 batch 쪽에서 다시 세운다.</b>
 *
 * <p><b>봉투는 {@code core} 로 올렸다.</b> 처음에는 batch 전용 레코드를 따로 뒀는데,
 * 같은 규약을 두 곳이 각자 정의하면 언젠가 어긋난다 — 저장소 규약도 <i>"응답은 항상
 * {@code ResponseEnvelope} 로 감싼다"</i> 로 한 벌을 요구한다. {@code api} 모듈의 파일을
 * 옮기는 것이 그 규약을 지키는 유일한 길이었다.
 *
 * <p><b>메시지는 카탈로그 것만 나간다.</b> 예외의 {@code detail} 은 로그에만 남긴다 —
 * {@code server.error.include-stacktrace: never} 와 같은 규율이고, 이 API 는 인증이
 * 없으므로 내부 사정을 더 조심해서 다룬다.
 */
@RestControllerAdvice(assignableTypes = VerifyTriggerController.class)
public class BatchApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(BatchApiExceptionHandler.class);

    private final TimeProvider timeProvider;

    public BatchApiExceptionHandler(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
    }

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleBusiness(BusinessException exception) {
        ErrorCode code = exception.getErrorCode();
        // 4xx 는 요청이 잘못된 것이라 사건이 아니다. 5xx 만 스택을 남긴다 —
        // 안 가르면 트리거를 잘못 부른 것만으로 ERROR 로그가 쌓여 진짜가 묻힌다.
        //
        // detail 은 **로그에만** 남긴다. 클라이언트에 나가는 문구는 errorCode.getMessage()
        // 다 — 저장소 규약이고, 이 API 에는 인증이 없어 더 지켜야 한다. detail 에는
        // 설정 키·가드 이름·실행 id 가 들어간다.
        if (code.getStatus() >= 500) {
            log.error("검증 API 가 실패했습니다. code={} detail={}",
                    code.getCode(), exception.getMessage(), exception);
        } else {
            log.info("검증 API 가 요청을 거절했습니다. code={} detail={}",
                    code.getCode(), exception.getMessage());
        }
        return respond(code, code.getStatus());
    }

    /**
     * <b>타입이 안 맞는 값은 400 이다.</b> {@code dataset=NOPE}·{@code asOf=garbage} 같은
     * 것들인데, {@code MethodArgumentTypeMismatchException} 은 {@code TypeMismatchException}
     * 만 상속하고 <b>{@code ErrorResponse} 를 구현하지 않는다</b>(바이트코드로 확인).
     * 그래서 아래 갈래로 새면 500 이 되고, 인증이 없는 이 API 에서 <b>누구나 ERROR 로그에
     * 스택트레이스를 원하는 만큼 쌓을 수 있다.</b>
     *
     * <p>같은 "잘못된 요청" 인데 <i>값 누락</i>은 400, <i>타입 불일치</i>는 500 으로 갈리는
     * 것도 문제다 — {@code MissingServletRequestParameterException} 은 그 인터페이스를
     * 구현해서 아래에서 걸린다.
     */
    @ExceptionHandler(org.springframework.beans.TypeMismatchException.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleTypeMismatch(
            org.springframework.beans.TypeMismatchException exception) {
        log.info("검증 API 가 요청을 거절했습니다. detail={}", exception.getMessage());
        return respond(CommonErrorCode.INVALID_INPUT, 400);
    }

    /**
     * <b>잡히지 않은 것은 전부 여기로 온다.</b> 이 갈래가 없으면 스프링 기본 오류 페이지가
     * 나가는데, 그 본문은 위 응답과 모양이 다르다 — 클라이언트가 두 형식을 다뤄야 한다.
     *
     * <p><b>스프링이 이미 상태를 정한 것은 그것을 살린다.</b> 파라미터 누락·타입 불일치는
     * {@link org.springframework.web.ErrorResponse} 를 구현한 예외로 오는데, 전부 500 으로
     * 접으면 <b>요청이 잘못된 것과 서버가 깨진 것이 클라이언트에게 같은 것으로 보인다.</b>
     * 실제로 그렇게 만들었다가 <i>"asOf 가 없으면 400"</i> 테스트가 잡았다.
     *
     * <p>{@code @ExceptionHandler} 에 그 인터페이스를 직접 못 쓴다 — {@code Throwable} 이
     * 아니기 때문이다. 그래서 여기서 가른다.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ResponseEnvelope<Void>> handleUnexpected(Exception exception) {
        if (exception instanceof org.springframework.web.ErrorResponse error) {
            int status = error.getStatusCode().value();
            if (status < 500) {
                log.info("검증 API 가 요청을 거절했습니다. status={} detail={}",
                        status, exception.getMessage());
                return respond(CommonErrorCode.INVALID_INPUT, status);
            }
        }
        log.error("검증 API 에서 예상 못 한 오류가 났습니다.", exception);
        return respond(CommonErrorCode.INTERNAL_ERROR);
    }

    private ResponseEntity<ResponseEnvelope<Void>> respond(ErrorCode code) {
        return respond(code, code.getStatus());
    }

    private ResponseEntity<ResponseEnvelope<Void>> respond(ErrorCode code, int status) {
        // requestId 는 null 이다 — batch 에는 그것을 MDC 에 심는 RequestIdFilter 가 없다.
        return ResponseEntity.status(status)
                .body(ResponseEnvelope.fail(ErrorResponse.of(code, null,
                        timeProvider.now().toInstant(java.time.ZoneOffset.UTC))));
    }

}

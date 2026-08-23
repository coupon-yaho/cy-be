// batch 업무 포트의 에러를 도메인 에러코드로 옮깁니다.
package com.kafkick.batch.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import com.kafkick.core.support.TimeProvider;
import com.kafkick.core.support.response.ErrorResponse;
import com.kafkick.core.support.response.ResponseEnvelope;
import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;
import com.kafkick.core.support.exception.ErrorCode;

/**
 * <b>이게 없으면 모든 도메인 예외가 500 으로 나간다.</b> {@code BusinessException} 이
 * {@code ErrorCode.getStatus()} 를 들고 있어도, 그것을 읽어 HTTP 로 옮기는 것이 없으면
 * 스프링은 그냥 서버 오류로 접는다 — <i>"만료가 도는 중이라 거절"</i>(409)과
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
 * <p><b>이 advice 가 못 잡는 4xx 가 있다.</b> {@code assignableTypes} 로 이 컨트롤러에
 * 묶여 있어서, <b>컨트롤러가 정해지기 전에 나는 예외</b>는 여기 안 온다 — 없는 경로(404)나
 * 허용 안 된 메서드(405)가 그것이다. 그때는 스프링 기본 형식
 * ({@code {timestamp,status,error,path}})이 나간다(실측). <b>클라이언트가 두 형식을 만날
 * 수 있다는 것은 사실이다.</b>
 *
 * <p><b>그 "다른 컨트롤러가 생기는 날" 이 왔다</b>(CY-429 의 {@code ExpireAdminController}).
 * 그래서 {@code assignableTypes} 에 둘을 함께 적는다 — <b>패키지 전체로 넓히지 않는다.</b>
 * 넓히면 다음에 붙는 컨트롤러가 이 규약을 <b>의식하지 않고도</b> 따라오는데, 그때
 * 어긋나는 것은 <i>"에러 형식이 두 벌"</i> 이라 클라이언트에서만 드러난다.
 * 이름을 하나씩 적으면 새 컨트롤러를 만든 사람이 <b>이 파일을 열어 보게 된다.</b>
 *
 * <p><b>메시지는 카탈로그 것만 나간다.</b> 예외의 {@code detail} 은 로그에만 남긴다 —
 * {@code server.error.include-stacktrace: never} 와 같은 규율이고, 이 API 는 인증이
 * 없으므로 내부 사정을 더 조심해서 다룬다.
 */
@RestControllerAdvice(assignableTypes = {VerifyTriggerController.class,
        ExpireAdminController.class})
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
            log.error("batch admin API 가 실패했습니다. code={} detail={}",
                    code.getCode(), exception.getMessage(), exception);
        } else {
            log.info("batch admin API 가 요청을 거절했습니다. code={} detail={}",
                    code.getCode(), exception.getMessage());
        }
        return respond(code);
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
        // getMessage() 는 **요청값을 그대로 담는다.** 이 API 의 파라미터에 PII 는 없지만,
        // 값을 로그에 남기는 습관이 남으면 다음 컨트롤러에서도 그렇게 한다 —
        // 규율은 "detail 에 PII 를 넣지 않는다" 다. 진단에 필요한 것은 이름과 타입이다.
        if (exception instanceof MethodArgumentTypeMismatchException mismatch) {
            log.info("batch admin API 가 요청을 거절했습니다. 파라미터={} 기대타입={}",
                    mismatch.getName(), mismatch.getRequiredType());
        } else {
            log.info("batch admin API 가 요청을 거절했습니다. 파라미터 타입이 맞지 않습니다.");
        }
        return respond(CommonErrorCode.INVALID_INPUT);
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
                // 같은 이유로 메시지를 안 남긴다 — 그 안에 요청값이 들어간다.
                log.info("batch admin API 가 요청을 거절했습니다. status={} type={}",
                        status, exception.getClass().getSimpleName());
                // INVALID_INPUT(400) 고정이라 헤더와 본문이 갈릴 것 같지만, **이 갈래로
                // 오는 4xx 는 전부 400 이다.** 스프링이 내는 404·405·406 은 컨트롤러가
                // 정해지기 전에 나서 assignableTypes 로 묶인 이 advice 에 도달하지 않고,
                // 415 는 이 API 가 body 를 안 받아 발생 자체가 없다 — 넷 다 실측했다.
                // 우리 도메인 404(VERIFY_EXECUTION_NOT_FOUND)는 handleBusiness 가 잡고
                // 그쪽은 코드 자신이 상태를 들고 있어 어긋날 여지가 없다.
                return respond(CommonErrorCode.INVALID_INPUT);
            }
        }
        log.error("batch admin API 에서 예상 못 한 오류가 났습니다.", exception);
        return respond(CommonErrorCode.INTERNAL_ERROR);
    }

    private ResponseEntity<ResponseEnvelope<Void>> respond(ErrorCode code) {
        int status = code.getStatus();
        // requestId 는 null 이다 — batch 에는 그것을 MDC 에 심는 RequestIdFilter 가 없다.
        return ResponseEntity.status(status)
                .body(ResponseEnvelope.fail(ErrorResponse.of(code, null,
                        timeProvider.now().toInstant(java.time.ZoneOffset.UTC))));
    }

}

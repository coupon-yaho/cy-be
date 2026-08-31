package com.kafkick.api.support.auth;

/**
 * 요청 헤더가 계약을 어겼습니다. <b>메시지가 응답에 그대로 나갑니다.</b>
 *
 * <p>{@code BusinessException} 을 쓰지 않는 이유가 그것입니다. 그쪽은 detail 을 로그에만
 * 남기고 응답에는 {@code errorCode} 카탈로그 문구만 싣습니다 — detail 에 식별자가 들어 있을
 * 수 있어서 맞는 규칙이지만, 헤더 계약 위반은 <b>호출자가 무엇을 고쳐야 하는지 응답만 보고
 * 알아야 하는 자리</b>라 그 규칙이 반대로 작용합니다.
 *
 * <p>실제로 그렇게 됐습니다. 등급 헤더 이름이 게이트웨이와 어긋난 동안 양쪽이 받은 것은
 * {@code "잘못된 요청입니다."} 뿐이었고, 원인을 짚는 데 양쪽 담당자가 한참 걸렸습니다.
 *
 * <p><b>메시지에는 코드가 정한 고정 문구만 담습니다.</b> 헤더 <i>값</i>은 회원 식별자나
 * 등급이라 넣지 않습니다 — 제약 애노테이션이 선언한 문구를 응답에 싣는 것과 같은 등급이고,
 * {@code GlobalExceptionHandler} 클래스 주석의 경계("요청에서 온 값은 안 되비춘다")를
 * 그대로 따릅니다.
 */
public class RequestHeaderContractException extends RuntimeException {

    /**
     * @param message 응답에 그대로 나가는 문구. <b>요청에서 온 값을 넣지 마십시오.</b>
     */
    public RequestHeaderContractException(String message) {
        super(message);
    }
}

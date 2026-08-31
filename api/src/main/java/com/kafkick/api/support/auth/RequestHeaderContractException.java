package com.kafkick.api.support.auth;

import com.kafkick.core.support.exception.BusinessException;
import com.kafkick.core.support.exception.CommonErrorCode;

/**
 * 요청 헤더가 계약을 어겼습니다. <b>사유 문구가 응답에 그대로 나갑니다.</b>
 *
 * <p>보통의 {@link BusinessException} 은 detail 을 로그에만 남기고 응답에는 카탈로그 문구만
 * 싣습니다 — detail 에 식별자가 들어 있을 수 있어서 맞는 규칙입니다. 헤더 계약 위반은 반대로
 * <b>호출자가 무엇을 고쳐야 하는지 응답만 보고 알아야 하는 자리</b>라, 전용 처리기가 이
 * 타입만 골라 사유를 그대로 싣습니다.
 *
 * <p>실제로 그렇게 됐습니다. 등급 헤더 이름이 게이트웨이와 어긋난 동안 양쪽이 받은 것은
 * {@code "잘못된 요청입니다."} 뿐이었고, 원인을 짚는 데 양쪽 담당자가 한참 걸렸습니다.
 *
 * <h2>사유를 {@link Reason} 으로 못박는 이유</h2>
 *
 * <p><b>문구를 문자열로 받으면 그 약속이 주석에만 있습니다.</b> 이 값은 응답과 로그에
 * 그대로 나가므로, 누군가 헤더 <i>값</i>이나 개인정보를 넣는 순간 그대로 새어 나갑니다.
 * 열거형으로 두면 <b>고정 문구만 나갈 수 있다는 것이 타입으로 강제</b>됩니다.
 *
 * <p>{@link BusinessException} 을 상속하는 것도 이유가 있습니다. 발급 컨트롤러가
 * {@code BusinessException} 은 <b>입력 거절</b>로, 그 밖의 {@code RuntimeException} 은
 * <b>예기치 못한 장애</b>로 집계합니다. 헤더 계약 위반은 클라이언트 잘못이라 장애 지표에
 * 섞이면 안 됩니다 — 부하 측정의 에러율이 그만큼 오염됩니다.
 */
public class RequestHeaderContractException extends BusinessException {

    /** 응답에 나갈 수 있는 사유 전부. <b>요청에서 온 값은 여기 못 들어온다.</b> */
    public enum Reason {

        MISSING_MEMBER_GRADE("회원 등급 헤더가 없습니다."),
        MULTIPLE_MEMBER_GRADE("회원 등급 헤더는 하나의 값만 허용합니다."),
        UNKNOWN_MEMBER_GRADE("지원하지 않는 회원 등급입니다.");

        private final String message;

        Reason(String message) {
            this.message = message;
        }

        public String message() {
            return message;
        }
    }

    public RequestHeaderContractException(Reason reason) {
        super(CommonErrorCode.INVALID_INPUT, reason.message());
    }
}

package com.kafkick.core.support.exception;

/** 도메인에 속하지 않는 횡단 에러. 쿠폰 등 도메인별 코드는 ErrorCode 를 구현한 별도 enum 으로 만든다. */
public enum CommonErrorCode implements ErrorCode {

    INVALID_INPUT(400, "COMMON-001", "잘못된 요청입니다."),
    NOT_FOUND(404, "COMMON-002", "요청한 리소스를 찾을 수 없습니다."),
    METHOD_NOT_ALLOWED(405, "COMMON-003", "지원하지 않는 요청 방식입니다."),
    INTERNAL_ERROR(500, "COMMON-004", "일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."),

    /**
     * 관리자 API 토큰 관문이 막았다(CY-742). <b>도메인이 아니라 여기 있는 이유</b>는 그 관문이
     * {@code /api/v1/admin/**} 전체에 걸리기 때문이다 — 검증뿐 아니라 {@code cleanup}·
     * {@code expire} 요청도 이 코드로 거절된다. 검증 도메인 코드로 두면 도메인별 집계가
     * 실제 오류 영역과 어긋난다.
     *
     * <p><b>메시지는 어느 쪽으로 틀렸는지 안 가른다</b> — 없는 것과 틀린 것을 가르면 그 자체가
     * 힌트가 된다. 그 구분은 로그에만 남는다.
     */
    UNAUTHORIZED(401, "COMMON-005", "관리자 토큰이 필요합니다.");

    private final int status;
    private final String code;
    private final String message;

    CommonErrorCode(int status, String code, String message) {
        this.status = status;
        this.code = code;
        this.message = message;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMessage() {
        return message;
    }
}

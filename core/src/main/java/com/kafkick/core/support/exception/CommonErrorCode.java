package com.kafkick.core.support.exception;

/** 도메인에 속하지 않는 횡단 에러. 쿠폰 등 도메인별 코드는 ErrorCode 를 구현한 별도 enum 으로 만든다. */
public enum CommonErrorCode implements ErrorCode {

    INVALID_INPUT(400, "COMMON-001", "잘못된 요청입니다."),
    NOT_FOUND(404, "COMMON-002", "요청한 리소스를 찾을 수 없습니다."),
    METHOD_NOT_ALLOWED(405, "COMMON-003", "지원하지 않는 요청 방식입니다."),
    INTERNAL_ERROR(500, "COMMON-004", "일시적인 오류가 발생했습니다. 잠시 후 다시 시도해 주세요."),
    FORBIDDEN(403, "COMMON-005", "접근 권한이 없습니다.");

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

package com.kafkick.api.admin.support;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * 관리자 HTTP API 선구축 단계에서 사용하는 API 계층 오류 코드입니다.
 *
 * <p>도메인 규칙 위반이 아니라 Controller의 실제 Use Case가 아직 연결되지 않았음을 표현하므로
 * core 도메인 오류와 분리합니다. Controller는 이 코드를 담은 {@code BusinessException}만 발생시키고,
 * 공통 {@code GlobalExceptionHandler}가 HTTP 상태와 실패 {@code ResponseEnvelope}를 생성합니다.</p>
 */
public enum AdminApiErrorCode implements ErrorCode {

    /** URL·입력·응답 타입은 존재하지만 실제 조회 또는 명령 구현이 연결되지 않은 상태입니다. */
    NOT_IMPLEMENTED(
            501,
            "ADMIN-001",
            "관리자 API 구현이 아직 연결되지 않았습니다."
    ),

    /** 관리자 역할이 없거나 정확한 {@code ADMIN} 값이 아닌 요청입니다. */
    FORBIDDEN(
            403,
            "ADMIN-002",
            "관리자 권한이 필요합니다."
    );

    private final int status;
    private final String code;
    private final String message;

    AdminApiErrorCode(int status, String code, String message) {
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

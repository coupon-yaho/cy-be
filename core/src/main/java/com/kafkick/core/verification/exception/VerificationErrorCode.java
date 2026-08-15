// 검증 배치의 도메인 에러입니다. core.support.ErrorCode 규약(<도메인>-<3자리>)을 따릅니다.
package com.kafkick.core.verification.exception;

import com.kafkick.core.support.exception.ErrorCode;

/**
 * 배치는 HTTP 서버가 아니지만 관리 포트로 verify 트리거 API 를 열기 때문에 status 를 채웁니다.
 * 배치 내부에서만 발생하는 오류는 500 을 씁니다.
 */
public enum VerificationErrorCode implements ErrorCode {

    INVALID_AS_OF(
            400,
            "VERIFICATION-001",
            "검증 기준 시각이 올바르지 않습니다."
    ),
    INVALID_RUN_PARAMS(
            400,
            "VERIFICATION-002",
            "검증 실행 파라미터가 올바르지 않습니다."
    ),
    RUN_NOT_FOUND(
            404,
            "VERIFICATION-003",
            "검증 실행을 찾을 수 없습니다."
    ),
    ASOF_STATE_MISSING(
            500,
            "VERIFICATION-004",
            "asOf 시점 상태가 준비되지 않았습니다."
    ),
    ILLEGAL_REPLAY_STATE(
            500,
            "VERIFICATION-005",
            "이력 리플레이 중 처리할 수 없는 상태를 만났습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    VerificationErrorCode(int status, String code, String message) {
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

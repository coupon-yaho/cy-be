package com.kafkick.core.consistency;

import com.kafkick.core.support.exception.ErrorCode;

/** 정합성 원천 검증과 계산 과정에서 발생하는 도메인 오류입니다. */
public enum ConsistencyErrorCode implements ErrorCode {

    /** 정합성 gap으로 해석할 수 없는 원천 상태가 입력되었습니다. */
    INVALID_SOURCE_STATE(
            400,
            "CONSISTENCY-001",
            "정합성 계산에 사용할 수 없는 원천 상태입니다."
    ),

    /** FINAL 판정에 필요한 원천값이 아직 유효하지 않습니다. */
    FINAL_VALUE_UNAVAILABLE(
            409,
            "CONSISTENCY-002",
            "최종 정합성 판정에 필요한 값이 준비되지 않았습니다."
    ),

    /** 원천값의 차이가 {@code long} 표현 범위를 벗어났습니다. */
    CALCULATION_OVERFLOW(
            500,
            "CONSISTENCY-003",
            "정합성 계산 중 수치 범위를 초과했습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    /**
     * 정합성 오류 코드를 생성합니다.
     *
     * @param status HTTP 상태 코드
     * @param code 외부에 노출할 안정적인 오류 식별자
     * @param message 외부 응답에 사용할 오류 메시지
     */
    ConsistencyErrorCode(int status, String code, String message) {
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

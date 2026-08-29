package com.kafkick.core.admin.overview;

import com.kafkick.core.support.exception.ErrorCode;

/** 관리자 운영현황 조립 과정의 안정적인 오류 응답 계약입니다. */
public enum AdminOverviewErrorCode implements ErrorCode {

    /** 관측 원천이 현재 요청과 다른 모집단 또는 평가 시각의 결과를 반환했습니다. */
    OBSERVATION_REQUEST_MISMATCH(
            500,
            "OVERVIEW-001",
            "운영현황 관측 결과를 처리할 수 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    /** HTTP 상태와 외부 오류 계약을 보존합니다. */
    AdminOverviewErrorCode(int status, String code, String message) {
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

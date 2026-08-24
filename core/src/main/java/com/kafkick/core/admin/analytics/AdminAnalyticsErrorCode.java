package com.kafkick.core.admin.analytics;

import com.kafkick.core.support.exception.ErrorCode;

/** 관리자 브랜드 분석 원천과 계산 과정의 안정적인 오류 응답 계약입니다. */
public enum AdminAnalyticsErrorCode implements ErrorCode {

    /** 조회 조건과 다른 집계 또는 계산할 수 없는 원천 설정이 제공됐습니다. */
    SOURCE_CONTRACT_MISMATCH(
            500,
            "ANALYTICS-001",
            "관리자 분석 원천 결과를 처리할 수 없습니다."
    );

    private final int status;
    private final String code;
    private final String message;

    /** HTTP 상태와 외부 오류 코드를 보존합니다. */
    AdminAnalyticsErrorCode(int status, String code, String message) {
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

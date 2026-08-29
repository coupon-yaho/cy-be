package com.kafkick.core.admin.analytics;

import com.kafkick.core.support.exception.ErrorCode;

/** 관리자 브랜드 분석 원천과 계산 과정의 안정적인 오류 응답 계약입니다. */
public enum AdminAnalyticsErrorCode implements ErrorCode {

    /** 조회 조건과 다른 집계 또는 계산할 수 없는 원천 설정이 제공됐습니다. */
    SOURCE_CONTRACT_MISMATCH(
            500,
            "ANALYTICS-001",
            "관리자 분석 원천 결과를 처리할 수 없습니다."
    ),

    /** 요청한 브랜드가 확인된 분석 카탈로그에 존재하지 않습니다. */
    BRAND_NOT_FOUND(
            404,
            "ANALYTICS-002",
            "요청한 브랜드를 찾을 수 없습니다."
    ),

    /** 요청한 캠페인이 확인된 분석 카탈로그에 존재하지 않습니다. */
    CAMPAIGN_NOT_FOUND(
            404,
            "ANALYTICS-003",
            "요청한 캠페인을 찾을 수 없습니다."
    ),

    /** 요청한 캠페인이 함께 지정한 브랜드에 속하지 않습니다. */
    CAMPAIGN_BRAND_MISMATCH(
            404,
            "ANALYTICS-004",
            "캠페인이 요청 브랜드에 속하지 않습니다."
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
